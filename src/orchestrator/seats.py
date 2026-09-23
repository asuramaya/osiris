"""Seat identity: an identity that exists before its first session.

The bug class this cures is structural: the durable identity key used to be
`agent:<session-id>`, a fact about a conversation, the most ephemeral object in the
system. Everything that matters (seat, house, charter, mail) was layered on top of that
key as assertions, reconstructed by inference at every entry point (whisper, mount,
re-attach, heartbeat). Every inference entry point could mint a new identity, and
patching each one individually (mint-lock, debounce, null-seam gate, dating gate, fork
archaeology, claimed-sid guard, the binding rule) kept defending the same underlying
wound. The fix is to stop inferring identity at all: a Seat is minted once as
`seat:<uuid8>` and never re-keyed. Its handle, house, and anchor are mutable assertions,
never the key itself, since keying identity on a mutable fact is exactly what caused the
bug class. A session attaches to a seat with a one-time token its spawner exported at
birth, before the session's own startup runs.

The attach protocol, and why each rule exists:
  * A token is one-time and binds to its first presenter. A subagent inherits its
    parent's environment (the CLAUDE_JOB_DIR lesson), so a second session presenting an
    already-used token is refused loudly: mount must refuse and say so when the anchor
    contradicts the claim, since silence was the original bug.
  * The same presenter re-presenting the token is a resume, not an intruder: an
    idempotent re-affirm.
  * A seat with a live session bound to it is not vacant. Two sessions in one seat is
    the collision this exists to prevent, refused before anything is written.
  * Tokens live in a plain table (hot secrets, revocable), never in the append-only
    kernel.

The mind layer keeps its own generation seam (swaps/compactions mint a new mind, and a
numeral tracks which one), and the binding follows the lineage head at every mint
(`follow_binding`, called from mint_heir), so the seat is the stable address precisely
because minds churn underneath it.
"""

from __future__ import annotations

import logging
import re
import secrets
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import asyncpg

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.seats")

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)

# How fresh a bound session's pulse must be to count as live contention. Mirrors the
# 15-minute liveness window used everywhere else: resolve_seat, agent_liveness, the roster.
_LIVE_SECS = 900

# Not an authorization boundary, and cannot be one: any agent that can run a shell can
# present any of these strings to a CLI command or by importing this module directly. The
# check is plain string equality; nothing behind it verifies who is actually calling.
# What these sentinels do buy is deliberateness and attribution: a caller must know and
# deliberately type one of them, and the crossing's own audit stamp (mintseat.py's
# `source=actor`) records exactly which one. That is a real, useful signal, just not a
# guarantee that the caller truly is the operator. This set lived first as mintseat.py's
# private cross-house-mint guard, then moved up here (mintseat.py now imports it from
# here) so derive_house's own house-anchor check shares the same definition, since two
# independent notions of "the operator's hand" would drift the moment one changed. A
# real authorization boundary would need the check to live somewhere an agent cannot
# reach at all, a different, larger design than this set.
_OPERATOR_ACTORS = {"operator", "analyst:operator", "console"}

# Lineage is per seat, not per actor: a self-managed seat's `--actor` is who typed
# `osiris new`, never a fact about which agent lineage the seat belongs to. But
# `_seat_lineage_ancestor` (trigger.py) reads the Seat's own `handle` assertion source
# as its founding lineage. Two seats founded under the same --actor used to carry the
# same source on that assertion, so the second seat's first launch would walk the
# actor's own live lineage forward and silently adopt whatever generation it found
# there (measured live: a seat in the resume_seat acceptance test inherited a sibling
# seat's own dormant session, both founded under the same actor). found_seat stamps
# this prefix plus the seat's own globally-unique handle as the source instead,
# guaranteed distinct per seat, and excluded here the same way _OPERATOR_ACTORS already
# is, so it falls through to the "no ancestor, mint a fresh `agent:seat-<id>` root" case
# every fresh self-managed seat is supposed to get. The actor itself is kept, but only
# as an attribution property (`founded_by`), never as a value anything downstream
# treats as lineage.
_FOUNDER_SOURCE_PREFIX = "founder:"


# Fleet-wide wedge found in production: pg_advisory_lock on a borrowed pool connection
# relies on a Python finally to unlock. A cancelled or wedged coroutine can return the
# connection to the pool still holding the lock (advisory locks are session-scoped, not
# reset by asyncpg's connection.reset()), and the next unrelated borrower inherits it.
# Every advisory lock in this file is now xact-scoped (pg_advisory_xact_lock inside an
# explicit transaction): the lock dies with the transaction. COMMIT, ROLLBACK, or the
# connection dropping on cancellation all release it, no finally required. A short SET
# LOCAL lock_timeout means a genuinely wedged holder makes waiters fail loud with a
# named error instead of silently parking for however long the fleet keeps retrying.
_LOCK_TIMEOUT = "5s"


class LockWedged(TimeoutError):
    """An advisory lock wasn't acquired within lock_timeout. Named so a caller (or the
    fleet reading the error) can tell "someone else is genuinely still working this key"
    apart from a generic timeout or a connection failure."""


@asynccontextmanager
async def _seat_lock(pool: asyncpg.Pool, house: str, handle: str) -> AsyncIterator[None]:
    """Serialize ensure_seat per (house, handle): the same advisory-lock discipline as
    mint_lock (two concurrent ensures would otherwise both find nothing and mint twins)."""
    key = f"seat:{house or ''}/{handle.lower()}"
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
        try:
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", key)
        except asyncpg.exceptions.LockNotAvailableError as exc:
            raise LockWedged(
                f"_seat_lock: {key!r} still held past {_LOCK_TIMEOUT}, another "
                "ensure_seat is genuinely in flight (or wedged)") from exc
        yield


@asynccontextmanager
async def _peer_lock(pool: asyncpg.Pool, *canonicals: str) -> AsyncIterator[None]:
    """Serialize peer_seats per seat: the same advisory-lock discipline as _seat_lock.
    peer_seats' own "already has a peer" check and its create_link are two round-trips
    with no lock between them, so two concurrent peer_seats calls sharing a seat could
    both pass the check and each mint an edge, leaving that seat with two active peer_of
    bonds, exactly what the chain design (see this file's own peer_seats docstring) says
    never happens. Locks every seat named, in sorted order, so overlapping pairs (a
    peer_seats(A, B) racing a peer_seats(B, C)) always acquire their shared seat (B) in
    the same order and never deadlock against each other."""
    keys = sorted({f"peer:{c}" for c in canonicals})
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
        for key in keys:
            try:
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", key)
            except asyncpg.exceptions.LockNotAvailableError as exc:
                raise LockWedged(
                    f"_peer_lock: {key!r} still held past {_LOCK_TIMEOUT}, another "
                    "peer_seats is genuinely in flight (or wedged)") from exc
        yield


async def find_seat(pool: asyncpg.Pool, *, house: str | None, handle: str) -> str | None:
    """The existing Seat object for (house, handle), by winning assertions: the same
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
    """Every active Seat carrying this handle, case-insensitive, globally. Deliberately
    house-agnostic: find_seat's (house, handle) lookup misses a real, unambiguous seat
    whenever the caller's own computed house doesn't match what's actually stored on it
    (a vacant seat has no live session to disagree with a stale or cwd-derived house
    guess). That gap once let claim_name mint a second seat for a handle while the real
    one, managed by another seat under a different house, sat untouched. Zero results
    means genuinely new (mint fresh, house-scoped is fine, nothing to conflict with);
    exactly one is the seat any claim to this handle must bind; two or more is an
    ambiguity that a caller must name and refuse, never silently arbitrate (fold_seat
    resolves it deliberately, this function only reports it)."""
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
    """Find-or-mint the Seat for a (house, handle): idempotent, advisory-locked. Minting is
    deliberate (called at a claim or a daemon spawn), never a bulk sweep: the graph heals
    forward, one seat at a time, as roles are actually exercised."""
    handle = (handle or "").strip()
    if not handle:
        return {"error": "a seat needs a handle"}
    async with _seat_lock(actions.pool, house or "", handle):
        existing = await find_seat(actions.pool, house=house, handle=handle)
        if existing is not None:
            return {"seat_id": existing, "handle": handle, "house": house, "minted": False}
        canonical = f"seat:{uuid.uuid4().hex[:8]}"
        # An 8-hex collision is vanishingly rare but a re-key is forbidden, so check and retry.
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
    """A one-time attach token for a seat, minted by the spawner and exported into the
    child's environment before the session's own startup runs (identity at birth,
    spec section 4.2)."""
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
    """A manually resumed session follows the seat binding: the holds link is the
    binding's durable half and survives session_end, while the mount row does not. A
    fresh row minted for a mind that actively holds a seat re-earns its `seat_id` from
    the link, no token needed, because the graph already knows who holds what.
    Idempotent and deliberately conservative: only a row with no binding is ever
    touched (an explicit attach, or a surviving row, always outranks a re-derivation)."""
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
    """The Seat this mind actively holds, with its display facts: the "who am I" half of
    the binding. orient/mount tell a bound mind which role it sits in, whether or not it
    ever claim_named itself in the assertion world. None when unbound.

    Lineage-aware: a seat-binding gap was once caught from two independent angles in the
    same hour, wake()'s authorization gate and the mail envelope's handle lookup.
    mint_heir's automatic succession never called claim_name, so a holds link minted for
    an ancestor generation was invisible to a caller asking about its successor, because
    the old query exact-matched the presented id. Now any active link anywhere in the
    lineage (the presented id, the bare root, or any `-<suffix>` generation, the same
    LIKE-prefix shape the trigger.py rate caps already use) answers; when more than one
    survives un-healed on the same seat, the newest generation wins, the same tiebreak
    follow_binding uses when it moves a link forward.

    A forked lineage is not the same case: rows naming more than one distinct seat means
    this lineage has split, a sibling generation wrongly grafted onto the same base (one
    seat's launch of another once minted a lineage's later generation as that new seat's
    own heir, a since-fixed defect in `_seat_lineage_ancestor`). A wrongly grafted sibling
    can easily out-number the true chain, so raw highest-generation-wins would hand a
    caller a seat some unrelated sibling holds, not its own. In that case only an exact
    match on the presented `agent_id` is trusted (the caller's own literal identity is
    the one fact that is never a guess); with no exact row of its own, the split is
    genuinely ambiguous and this returns None rather than silently picking a branch:
    unbound reads honestly, a wrong seat does not.

    `house` is derived, never the seat's own stored property. See derive_house()."""
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
    if len({r["seat_id"] for r in rows}) > 1:
        exact = [r for r in rows if r["holder"] == agent_id]
        if not exact:
            return None
        best = exact[0]
    else:
        best = max(rows, key=lambda r: _generation(r["holder"])[1])
    house = await derive_house(pool, best["seat_id"])
    return {"seat_id": best["seat_id"], "handle": best["handle"], "house": house}


async def held_seat_exact(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """`held_seat`'s exact-generation counterpart. Lineage-wide resolution (any
    generation sharing the base, newest wins) is right for a live mind asking "what
    seat do I sit in", but wrong for a third-party act on a specific named generation,
    like `retire_agent`'s own seat-vacate step. An ancestor already succeeded away (its
    own `holds` link long invalidated by `bind_holder`) shares its heir's base, so the
    lineage-wide query once resolved to the seat the heir currently, rightfully holds,
    and retire_agent then vacated it, evicting a live mind as a side effect of retiring
    a generation that held nothing of its own. Returns the seat canonical only when
    `agent_id` itself, no base, no `-%` widening, carries a live `holds` link; None
    otherwise, even if its lineage holds plenty."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT t.canonical FROM links l "
        "JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical=$1 AND l.type='holds' AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_id)


async def seat_by_handle(pool: asyncpg.Pool, handle: str) -> dict[str, Any] | None:
    """A bare handle to its Seat, by name alone (case-insensitive exact match), no
    liveness or holder involved: `held_seat`'s own counterpart for the case an agent id
    is not what the caller has. Built for the `team` console command's own `--seat`
    argument: a terminal has no mounted identity of its own to resolve team()'s
    self-scoped contract, so it names the manager by handle instead and this resolves it
    directly, the same "resolve then call the shared logic" shape cmd_stop's own operator
    lane already uses for trigger.stop_seat. None when no active seat carries that handle,
    or more than one does (a caller should not silently pick between two)."""
    rows = await pool.fetch(
        "SELECT t.canonical AS seat_id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=t.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle "
        "FROM objects t WHERE t.type='Seat' AND t.status='active' "
        "AND lower((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=t.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)) "
        "   = lower($1)",
        handle)
    if len(rows) != 1:
        return None
    seat_id = rows[0]["seat_id"]
    house = await derive_house(pool, seat_id)
    return {"seat_id": seat_id, "handle": rows[0]["handle"], "house": house}


async def tree_seat_hint(pool: asyncpg.Pool, *, cwd: str) -> str | None:
    """A project-tree cwd's own declared seat, by handle: an already-bound seat's own
    `tree_cwd` wins first (an established binding is more authoritative than a file
    line), else the `.osiris` pin's `seat = "<handle>"` line (agents.py's
    `read_seat_handle`, parseable since it was added but with zero callers until this).
    None when neither signal is present: a bare code checkout, nobody's declared seat,
    the ordinary project-only mount stays untouched."""
    row = await pool.fetchval(
        "SELECT (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='tree_cwd' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $1",
        cwd)
    if row:
        return str(row)
    from src.orchestrator.agents import read_seat_handle
    return read_seat_handle(cwd)


async def project_coordinator_seat(pool: asyncpg.Pool, project: str) -> str | None:
    """The seat that governs `project` and has no manager of its own: the same
    "unmanaged head" derivation `roster()`'s own shared-house branch already uses
    (manager_of_seat(pool, seat_id) is None), reused here rather than a second notion of
    "coordinator." None when no seat governs the project, or every seat that does is
    itself managed (an org-chart shape this function refuses to guess through)."""
    rows = await pool.fetch(
        "SELECT s.canonical FROM links l "
        "JOIN objects s ON s.id=l.from_id AND s.type='Seat' AND s.status='active' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' AND p.canonical=$1 "
        "WHERE l.type='governs' AND (l.valid_until IS NULL OR l.valid_until > now())",
        f"repo:{project}")
    for r in rows:
        if await manager_of_seat(pool, r["canonical"]) is None:
            return str(r["canonical"])
    return None


async def _seated_house(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The seat-first half alone, shared by `resolve_project` and
    mcp_server._resolve_project_seat_first: a seated agent's project is its seat's own
    charter when it has exactly one (a real, deliberately-declared `governs` edge),
    never guessed from cwd. Falls back to the seat's derived house only when no charter
    is declared, or when more than one is (an ambiguity this function refuses to
    arbitrate, same law as everywhere else in this house).

    A live production specimen: a seat's own house is its name, not necessarily its
    work. One seat's house was its own name, but its charter (and its office's own
    .osiris pin, already correctly fixed) named a different, unrelated repo. The
    unconditional house-as-project law this function used to enforce was written for
    the self-managed case, where a seat's house and its work happen to share a name, and
    it silently re-corrupted every other seat's project back to its own name on every
    single mount, forever. Not a one-time historical artifact: a standing bug, whose
    effect recurred days after a related anchor_cwd corruption was thought closed.

    Split out so mount()'s own wrapper can call only this half (its cwd guess already
    came from resolve_identity moments earlier, in the same pipeline, and must win
    untouched when this returns None; re-deriving a second, independent cwd guess here
    risks disagreeing with it, e.g. under a test's monkeypatched office root).

    Resolved through `project_current_name`: `charter_of`'s own entries are `governs`'s
    canonical, stripped of its `repo:` prefix, and stay canonical forever on purpose,
    but this function feeds `ident.project`, the live-displayed label every
    mount()/get_status()/orient() caller reads as "your project", and a canonical is a
    mint-time slug, not a name (a project's canonical stays its old slug even long after
    the project itself was renamed). Every other seat-project derivation this drift
    already touched (resync_seat_project, _is_ghost_house, sweep_seat_trees) already
    resolves through this same function; this was the one seat-first-mount path still
    reading the frozen canonical instead."""
    seat = await held_seat(pool, agent_id)
    if seat is None:
        return None
    from src.orchestrator.charter import charter_of, project_current_name
    repos = await charter_of(pool, seat["seat_id"])
    if len(repos) == 1:
        return await project_current_name(pool, repos[0])
    return str(seat["house"]) if seat.get("house") else None


async def resolve_and_persist_seated_project(
    actions: Actions, agent_id: str,
) -> str | None:
    """`_seated_house`, but also fixes a gap: mount()'s own
    `_resolve_project_seat_first` (and automount()'s counterpart) mutate the in-memory
    AgentIdentity's `.project`, which fixes that call's own receipt and the durable
    mount-registry row, but register_agent had already asserted (or skipped
    asserting, when cwd resolved to nothing) the Agent object's own `project` property
    moments earlier, and nothing ever went back to correct it. fleet() reads that
    assertion directly (`current_assertions WHERE name='project'`), never the registry
    row, so a seated agent whose cwd didn't independently resolve to its house (the
    bare seats container root is the canonical case) stayed filed under "?" in every
    fleet() call forever, even though orient()/mount() (which re-derive live via the
    seat binding on every call) told the correct story the whole time.

    Writes the same source/evidence shape register_agent's own project assertion uses
    (`src=agent_id`, SELF_DECLARED) so this correction properly supersedes within that
    source: one clean compensating write, not a second competing source muddying
    current_assertions' multi-source set. No-op (returns None, writes nothing) for an
    unseated agent: an honest "not seated" stays exactly what register_agent already
    wrote, never invented here."""
    house = await _seated_house(actions.pool, agent_id)
    if house is None:
        return None
    agent_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
    if agent_oid is not None:
        await actions.assert_property(agent_oid, "project", house, agent_id,
                                      datetime.now(UTC), _CONF, evidence_class=_EC)
    return house


async def resolve_project(
    pool: asyncpg.Pool, agent_id: str, cwd: str | None,
) -> str | None:
    """The one project resolver: every reader that needs "which project is this agent
    in" and has no cwd-derived guess of its own already funnels through here. The stop
    hook's four hand-rolled `Path(cwd).name` sites and census.live_bodies used to each
    re-derive their own answer. The specimen that forced the consolidation: a session's
    cwd was the bare seat-office container (~/.osiris/seats), basename-guessed "seats",
    a phantom project neither fleet() nor a mail query should ever see.

    A seated agent's project is its seat's derived house (`_seated_house`),
    unconditionally. This is also how `~/.osiris/seats/<handle>` resolves to the seat's
    house, not the handle: a caller that has already turned that directory into an
    agent_id (binding_of_handle, same as the stop hook's own identity resolution) gets
    the real house here, never the bare handle.

    An unseated agent falls back to a cwd-derived guess (a `.osiris` pin, else the
    folder's basename) except the bare office container itself
    (offices.is_bare_office_root), which refuses (None) rather than mint the "seats"
    phantom. Deliberately not the full `resolve_identity`: that also guesses a session
    id by scanning ~/.claude/projects for the
    hottest matching transcript, disk I/O this project-only lookup has no use for (and the
    stop hook's per-turn budget and census's per-pid loop can't afford). mount() does not use
    this fallback branch (see `_seated_house`'s own note): it already has resolve_identity's
    answer and only needs the seated override.

    Takes any connection-like with `.fetchval`/`.fetch` (a bare asyncpg.Connection works fine,
    not only a Pool, since the stop hook has no pool of its own, only `asyncpg.connect(DSN)`)."""
    house = await _seated_house(pool, agent_id)
    if house is not None:
        return house
    if not cwd:
        return None
    from src.orchestrator.agents import read_project_label
    from src.orchestrator.offices import is_bare_office_root

    pinned = read_project_label(cwd)
    if pinned:
        return pinned
    if is_bare_office_root(cwd):
        return None
    return Path(cwd).name or None


SeatState = Literal["vacant", "occupied", "cold"]


async def seat_occupancy(
    pool: asyncpg.Pool, seat_id: str, *, live_secs: int = _LIVE_SECS,
) -> dict[str, Any]:
    """Vacant / occupied / cold: one authority for whether a Seat has a living session
    in it, computed at read time from links(holds) + agent_mounts, no schema change, no
    new table. The acceptance case that drove this: one office once showed four bodies
    where one lived; a seat with no holder at all must read vacant on its own, never
    silently absent from a query that only ever asks about agents.

    Vacant: no `holds` link has ever existed for this seat (mint_seat's own law: a seat
    is furniture until a body sits in it). Occupied: an active holder exists and is live
    right now. Cold: held, now or in the past, but nobody live this instant: the
    ordinary in-between state of a seat between sessions, never an alarm by itself.

    Liveness itself is mounts.agent_liveness(), not a second copy of the same logic:
    this used to run its own inline agent_mounts-only query, the exact single-source
    shape the dispatch-listener probe had before it gained a current_assertions.
    last_active fallback, a repair carried to one reader and never to this one, the
    most expensive instance of that pattern because every human-facing occupancy figure
    (roster(), fleet()) reads through here. Delegating makes this more permissive than
    before (agent_liveness's freshest-of-two-signals can only turn a false "cold" into a
    true "occupied", never the reverse); the direction was checked against every caller
    before landing (see commit message).

    `live_secs` is no longer honored as a variable window: agent_liveness owns the one
    liveness window (LIVENESS_WINDOW_MINUTES) every reader now shares, and a second
    window threaded in here would only grow back into a second copy of the problem this
    fixes. Kept only for signature stability (nothing in this codebase varies it,
    confirmed by grep across src/ and tests/, zero non-default call sites); passing a
    non-default value raises rather than being silently dropped, so a future caller who
    actually needs a different window finds a refusal to design against, not a value
    that quietly stopped doing anything.

    Lineage-aware the same way held_seat is: the active holder's liveness is read across
    its whole lineage (base id or any `-<suffix>` generation) by agent_liveness itself,
    because bind_holder/follow_binding keep the holds link on the freshest generation but
    a mount row can still be tagged to an ancestor label mid-succession.

    This is the read launch() needs for its own idempotency (detect-existing-window
    before spawning, never mint a duplicate body) and its honest liveness receipt
    (body-exists and can-receive are separate states, each independently verifiable):
    call it with the seat about to be launched into, before launching."""
    if live_secs != _LIVE_SECS:
        raise ValueError(
            f"seat_occupancy no longer supports a custom live_secs ({live_secs!r}), "
            "liveness is delegated to mounts.agent_liveness()'s own shared window "
            f"(LIVENESS_WINDOW_MINUTES) so every reader agrees; the default ({_LIVE_SECS}) "
            "already matches it. A genuine need for a different window is a design "
            "question for mounts.py's shared helper, not a per-caller parameter here.")
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
    from src.orchestrator.mounts import agent_liveness

    live = (await agent_liveness(pool, holder))["live"]
    return {"state": "occupied" if live else "cold", "holder": holder, "live": live}


async def fleet_occupancy(
    pool: asyncpg.Pool, *, live_secs: int = _LIVE_SECS,
) -> list[dict[str, Any]]:
    """Every active Seat's occupancy, one row each: the batch read fleet() renders beside
    the agent tree, so a seat with no holder at all is as visible as one with several.
    Same authority as seat_occupancy(), run once per seat instead of asked one at a
    time; small fleet, small N, and matches backfill_unbound_seats' own list-then-resolve
    shape rather than a fused query the seat count doesn't yet justify.

    `house` is derived per seat. See derive_house()."""
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


_ROSTER_CAVEATS = (
    "chartered_repos and pin.declared are reported as-is, never certified canonical: minting "
    "a SoftwareProject is cheap and mostly ungated, so a name resolving to a real graph object "
    "proves the object exists, not that it is the current or correct name for a repo. Near-"
    "duplicate variants (the bytebye/byebyte history is the live example) can each "
    "independently look valid here. Canonicalizing project names is a separate concern, "
    "not this verb's: a caller that needs 'which of these names is right' asks there, not here.",
    "pin is read from anchor_cwd's own .osiris, or, when anchor_cwd is not recorded, from the "
    "conventional ~/.osiris/seats/<handle>/.osiris path when that probe finds one "
    "(probed_anchor_cwd names which). A seat with a distinct tree_cwd (the office/"
    "tree split) may carry its own, possibly different, .osiris there. This does not read "
    "it, and does not check the two agree.",
    "office_exists is a plain directory-existence check on anchor_cwd (or the probed "
    "conventional path when anchor_cwd is absent), nothing more. It does not mean the "
    "office's CLAUDE.md/charter.md content is actually being loaded by a live session. That "
    "question is separate: the office-content mechanism is mid-migration, so "
    "a seat can read office_exists=true with orphaned office content.",
    "the conventional-path probe (pin.state=\"unknown-office\" on a miss) checks exactly one "
    "path, ~/.osiris/seats/<handle>/, nothing more. A miss means neither the recorded "
    "anchor_cwd nor that one convention found an office; it is not a claim that no office "
    "exists anywhere.",
    "live_cwd (the current holder's own agent_mounts row, when occupied) can differ from both "
    "anchor_cwd and tree_cwd with nothing wrong on the launch path: seats have been found "
    "bound to a tree_cwd while their live sessions sit at the office cwd. "
    "The three are reported separately on purpose; none is silently treated as 'the' cwd.",
    "pin.triage_bucket only ever looks up repo:<pin.declared>. It inherits every one of "
    "pin's own blind spots above (not certified canonical, anchor_cwd-only) plus one more: "
    "a bucket reflects a single triage snapshot taken once per roster() call, not a live "
    "join, so it can go stale the instant something else in the graph changes after this "
    "call returns.",
    "pin.triage_bucket=='duplicate_suspect' is a PER-OBJECT condition (pure marks, no winner "
    "ever picked), not a verdict on the SEAT's own pin: it fires "
    "whenever ANY object shares this one's case-folded basename, even when the seat's own "
    "declared object is the real, populated one and the sibling is an empty, unrelated "
    "phantom (confirmed live with a werner/maat/till/aegis specimen). `pin.duplicate_siblings` "
    "(present only on this bucket) lists each "
    "colliding object's own canonical and agent_count SO A READER CAN JUDGE THE COLLISION "
    "THEMSELVES. It never picks a winner among them, it only stops attributing a sibling's "
    "emptiness to a seat whose own pin may be perfectly fine.",
    "pin.triage_bucket=='no-such-project' can carry `pin.name_resolution`, DIAGNOSTIC ONLY, "
    "never a resolution. A seat's `project` pin is the CANONICAL SUFFIX by system-wide "
    "contract (every mint/lookup path outside this note builds repo:<value> and mints a NEW "
    "object on a mismatch, as the khepri specimen confirmed); if the declared value instead "
    "matches some OTHER object's current NAME (a "
    "post-rename display name, not its canonical), that is worth saying but never worth "
    "silently trusting. `resolved_by:'name'` with a `canonical` names what the pin SHOULD "
    "be corrected to; `ambiguous:true` with `candidates` means it matches more than one "
    "object's name and picks none of them (a general rule). Absence of this field "
    "on a no-such-project pin means the value matches nothing at all, a genuinely dead pin.",
    "pin_charter_agreement compares only pin.declared against chartered_repos, and inherits "
    "every caveat above (not certified canonical, anchor_cwd-only, single snapshot). "
    "'disagree' names a conflict for a mind to resolve (rebind_seat/correct_pin_value/"
    "set_charter), it never picks which side is right (the same rule as triage_bucket "
    "above), a seat legitimately holding one pin while its charter covers several OTHER "
    "repos too still reads 'agree' as long as the pin's own value is among them. 'n/a' means "
    "there was nothing to compare (pin not declared, or no charter at all), never conflate "
    "with 'disagree'.",
)


def _dir_exists(path: str | None) -> bool | None:
    """A plain sync wrapper so `roster()` (async) never calls a blocking Path method inline
    (ASYNC240). None when there's no path to check, never a guess."""
    return Path(path).is_dir() if path else None


async def _triage_bucket_map(pool: asyncpg.Pool) -> dict[str, str]:
    """canonical -> bucket for every active SoftwareProject, one call shared across every
    row roster() builds, rather than a per-seat re-scan. Goes through
    `compositions.run_spec`, the same public entrypoint mcp_server.py's own `triage()`
    tool uses, never the module's private `_fn_triage`/`_triage_buckets` directly.

    Function-local import, deliberately: compositions.py imports this module at its own
    top (`_OPERATOR_ACTORS`), so a top-of-file import back from here would be a real
    cycle: Python would hit a partially-initialized module on whichever side loads first,
    breaking at process start, not at a call site. By the time this function actually
    runs, mcp_server.py has already finished importing both modules, so this is a plain
    `sys.modules` lookup, the same pattern mailbox.py already leans on roughly ten times
    for this exact shape (folds.py/agents.py/seats.py/capture.py, all function-local
    there).

    58 active SoftwareProject objects exist today (measured live), comfortably under the
    limit=2000 page this asks for, so this is exhaustive in practice, not a silent
    truncation. If that ever stops being true, the caller (roster) would start seeing
    `no-such-project` for a real, merely-unpaged project: the honest failure mode of a
    page-based read outgrowing its own page, not a wrong answer."""
    from src.orchestrator import compositions

    spec = {"op": "function", "name": "triage",
            "args": {"mode": "buckets", "object_type": "SoftwareProject",
                     "status": "active", "limit": 2000}}
    out = await compositions.run_spec(pool, spec, None, name="triage")
    return {row["canonical"]: row["bucket"] for row in out["items"] if "canonical" in row}


_BASENAME_FOLD_SQL = (
    "lower(CASE WHEN o.canonical LIKE '%/%' THEN regexp_replace(o.canonical, '^.*/', '') "
    "WHEN o.canonical LIKE '%:%' THEN regexp_replace(o.canonical, '^[^:]*:', '') "
    "ELSE o.canonical END)"
)


async def _duplicate_suspect_siblings(pool: asyncpg.Pool, canonical: str,
                                      ) -> list[dict[str, Any]]:
    """The other active SoftwareProject objects sharing `canonical`'s own case-folded
    basename: the exact collision `_triage_buckets`' `duplicate_suspect` bucket fires on
    (compositions.py's `per_object` CTE, same fold, reproduced here rather than imported
    to avoid a real import cycle; seats.py is already function-local-importing
    compositions elsewhere in this same file for the identical reason,
    `_triage_bucket_map`'s own docstring explains it). Each sibling's own `agent_count`
    rides along (the same `works_in`-edge count `project_ledger` already reports per
    project: this names a fact about each object independently, it never compares them
    to crown one canonical). Only called when a bucket is already `duplicate_suspect`,
    never a blind per-row cost on the common (non-flagged) path."""
    rows = await pool.fetch(
        f"WITH target AS ("
        f"  SELECT {_BASENAME_FOLD_SQL} AS basename FROM objects o "
        f"  WHERE o.canonical=$1 AND o.type='SoftwareProject' AND o.status='active') "
        f"SELECT o.id, o.canonical FROM objects o, target t "
        f"WHERE o.type='SoftwareProject' AND o.status='active' AND o.canonical != $1 "
        f"AND {_BASENAME_FOLD_SQL} = t.basename",
        canonical)
    siblings: list[dict[str, Any]] = []
    for r in rows:
        agent_count = await pool.fetchval(
            "SELECT count(DISTINCT l.from_id) FROM links l WHERE l.to_id=$1 "
            "AND l.type='works_in' AND (l.valid_until IS NULL OR l.valid_until > now())",
            r["id"])
        siblings.append({"canonical": r["canonical"], "agent_count": agent_count})
    return sorted(siblings, key=lambda s: s["canonical"])


async def _pin_name_resolution_note(pool: asyncpg.Pool, pin_value: str,
                                    ) -> dict[str, Any] | None:
    """A pin's `project` field is the canonical suffix by system-wide contract: every
    mint/lookup path outside this one (register_agent, mint_heir's own
    `_resolve_or_mint_project`) builds `repo:<value>` and treats a mismatch as grounds to
    mint a new object, never to resolve by name. So when roster()'s own canonical-only
    lookup already came back `no-such-project`, this asks one further, purely diagnostic
    question: does this value match some other live object's current name instead? If
    so, that is worth surfacing, since a pin holding a display name instead of a
    canonical is a real, previously-seen mistake, but it is never worth acting on here.
    This function never changes triage_bucket, never implies the pin should be silently
    rewritten to what it found, and never picks among multiple name matches:
    `_resolve_software_project`'s own `AmbiguousProjectRef` is surfaced as its own
    distinct state, not swallowed into a false single answer. A canonical match here
    (the resolved object's own canonical equals `repo:<pin_value>`) returns None:
    roster's own lookup would already have found that case, this has nothing new to
    add."""
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project

    try:
        row = await _resolve_software_project(pool, pin_value)
    except AmbiguousProjectRef as exc:
        return {"resolved_by": "name", "ambiguous": True, "candidates": exc.candidates}
    if row is None:
        return None
    canonical = row["canonical"]
    if canonical == f"repo:{pin_value}":
        return None
    return {
        "resolved_by": "name", "canonical": canonical,
        "note": ("this pin's value matches an existing object's current NAME, not its "
                 "canonical suffix. A pin should hold the canonical (stable across a "
                 "rename), never a display name; correct the pin to the canonical shown "
                 "here, never to the name it currently holds"),
    }


def _roster_caveats_out(caveats: list[str], want_caveats: bool) -> dict[str, Any]:
    """Trims the receipt: `_ROSTER_CAVEATS` alone is 10 standing paragraphs, printed on
    every call regardless of whether the caller has ever needed the text, measured as a
    significant contributor to response size. Default is a one-line pointer;
    `want_caveats=True` restores the full list, unchanged from before this trim."""
    if want_caveats:
        return {"caveats": caveats}
    return {
        "caveats_count": len(caveats),
        "caveats_note": (
            f"{len(caveats)} standing caveat(s) about this function's own blind spots, "
            "pass want_caveats=True for the full text"),
    }


async def roster(
    pool: asyncpg.Pool, *, repo: str | None = None, live_secs: int = _LIVE_SECS,
    want_caveats: bool = False,
) -> dict[str, Any]:
    """The roster: answers "which seat owns this repo" and "is a seat's project pin
    still pointing at something" from the graph, no `ls ~/.osiris/seats/*/.osiris`
    required. Built after a manager read mount()'s live-agent list as the roster, found
    every seat in their house cold, and read cold as vacant: work got misrouted to the
    wrong seat, and a separately owned repo got wrongly declared ownerless and edited
    directly, both while the seat offices on disk held the correct answer the whole
    time. This function makes that manual, error-prone query unnecessary.

    Cold is not vacant, the root cause, made structurally impossible to collapse here:
    each row's `occupancy` is `seat_occupancy`'s own vacant/occupied/cold, unchanged.
    Vacant means no `holds` link has ever existed for this seat; cold means held, now or
    in the past, but nobody live this instant; occupied means a live body is in it right
    now. A caller reading this dict cannot mistake the second for the first without
    discarding a field that is right there.

    Per seat: `chartered_repos` (charter_of's own `governs` links, self-declared,
    graph-native) and `pin` (a live read of anchor_cwd's `.osiris`, reusing
    `read_project_pin`'s own three-way declared/found-but-unset/could-not-read
    distinction rather than collapsing it). Neither is certified canonical; see
    `_ROSTER_CAVEATS`, always returned alongside the rows, because a roster that states
    its own blind spots is worth more than one that reports a clean answer over a graph
    that is wrong in several known ways right now. `office_exists` and `live_cwd` are
    separate, deliberately uncollapsed axes; see the caveats for exactly what each does
    and does not mean.

    `pin_charter_agreement` closes a gap that let real mis-bindings go unnoticed: a row
    already carried `pin.declared` and `chartered_repos` and nothing compared them, so a
    mis-binding surfaced only when someone happened to notice by hand. `'agree'`/
    `'disagree'`/`'n/a'` is a mark, never a verdict: a caller sees a conflict and
    resolves it with the existing repair verbs (rebind_seat/correct_pin_value/
    set_charter), this function never picks a side. `'n/a'`, not `'disagree'`, when
    there is nothing to compare: an unset pin is a valid state, not a defect, and an
    uncharted seat has no charter to disagree with.

    No `anchor_cwd` recorded is not the same claim as no office: several real,
    furnished seats used to read `no-office` with a null anchor, because that state was
    "confident about the world, derived from a null in the graph." One extra probe of
    the conventional path (`~/.osiris/seats/<handle>/`) runs only when `anchor_cwd` is
    absent; a hit surfaces via `probed_anchor_cwd` (kept separate from `anchor_cwd` so a
    reader always sees what the graph actually recorded vs what this function found by
    convention) and `pin`/`office_exists` read from it normally. A miss is
    `pin.state="unknown-office"`, never `no-office`: even after the probe, an office
    could still exist somewhere this function doesn't know to check.

    `pin.triage_bucket` is a third state, not two: `None` when there's nothing declared
    to look up (`pin.state` isn't `declared`); `"no-such-project"` when a project is
    declared but no active SoftwareProject object named `repo:<pin.declared>` exists in
    the graph (a dangling pin, not a triage verdict); otherwise the real bucket triage's
    own buckets mode computed for that object:
    `contradicted`/`duplicate_suspect`/`bulk_import`/`orphan`/`hub`/`stale`/`thin`/`normal`.
    Reuses triage verbatim (via `_triage_bucket_map`) rather than inventing a second
    project-health notion; a past complaint that "triage didn't help at all here" was a
    discoverability gap, not a missing capability.

    `repo=None` returns every active seat's row. `repo=<name>` instead answers the exact
    question "who owns this" by checking both signals independently: a seat whose
    charter or current pin names `repo` is a match, tagged with which signal(s) found
    it. Zero matches is `agreement="no-match"`, explicitly not the same claim as
    "nobody owns this repo": it means neither signal this function reads found an
    owner, which is the honest boundary of what a graph+pin read can say, not a verdict
    on the repo's actual ownership.

    Two seats matching (both patterns reproduced in real, live use):
    - the charter-seat manages the pin-seat (a real `managed_by` edge, checked via
      `manager_of_seat`) is `agreement="governed"`: a coordinator governing a repo its
      own worker sits in is the normal, correctly-configured shape, not a warning.
      Calling this `conflict` trained readers to skip the word; the one time it means
      two seats genuinely claiming the same repo by the same signal, it would get
      skipped too.
    - anything else (two charters, two pins, or an unrelated charter+pin pair) stays
      `agreement="conflict"`, returned as both rows, never silently resolved to one, now
      a word that means something.

    N>2 seats matching, the shared-house case: when every matched seat's own derived
    house (`derive_house`) is the same house, and that house's name is this repo, it's a
    house's own home repo: every worker legitimately charters/pins it, that's the
    normal shape, not several seats fighting over one thing. `agreement="shared-house"`,
    `manager` names the house's own unmanaged head seat (the one `manager_of_seat`
    returns None for): the seat a caller should prefer, the same role the charter-seat
    plays in `governed`. Anything else with N>2 matches (an unrelated cluster of seats
    that merely happen to share a repo string) stays plain `conflict`, `manager=None`.

    No literal match is a real answer, but a standing disclaimer printed on every
    no-match carries no information on the call where it's actually true. So on
    `no-match` only, one extra pass re-checks every seat's charter/pin case- and
    separator-insensitively (strip non-alphanumerics, lowercase) and returns
    `near_misses`, evidence, never promoted to a match:
    `{"repo": <as actually stored>, "seat", "via", "differs_by": "case"|"separator"}`.
    Reproduced live: a repo was renamed family-wide, but two seats' charter/pin still
    carried the old spelling, so a lookup by the new name returned a bare,
    uninformative `no-match` until this existed.

    `manager` is each row's own `manager_of_seat` resolved to the manager's `handle`
    (same handle-by-canonical read `seat_facts` already makes, reused rather than a
    second lookup), or `None` when the seat is unmanaged. Closes a gap where a stated
    promotion was a fact this function already had every fact to confirm:
    `seat(action='promote')` mints the `managed_by` edge, `manager_of_seat` already
    reads it (`governed`/`shared-house` above both call it), but no per-seat row ever
    surfaced it, so a reader checking a promotion by roster alone would call it failed
    with the edge sitting right there in the graph."""
    from src.orchestrator.agents import read_project_pin
    from src.orchestrator.charter import charter_of
    from src.orchestrator.offices import _default_office_root

    bucket_map = await _triage_bucket_map(pool)
    seat_rows = await pool.fetch(
        "SELECT o.canonical AS seat_id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle "
        "FROM objects o WHERE o.type='Seat' AND o.status='active' ORDER BY o.canonical")

    rows: list[dict[str, Any]] = []
    for sr in seat_rows:
        seat_id = sr["seat_id"]
        facts = await seat_facts(pool, seat_id)
        occ = await seat_occupancy(pool, seat_id, live_secs=live_secs)
        chartered = await charter_of(pool, seat_id)
        manager_seat_id = await manager_of_seat(pool, seat_id)
        manager_handle = (
            (await seat_facts(pool, manager_seat_id))["handle"]
            if manager_seat_id is not None else None)
        anchor = facts["anchor_cwd"]
        # `anchor is None` used to mean "no-office", a confident claim about the world
        # derived from a null in the graph. A review found every seat this collapsed had
        # a real, furnished office on disk at the conventional path
        # (~/.osiris/seats/<handle>/), invisible to roster() and, through it, to a
        # migration plan that reported far fewer gaps than actually existed because
        # those seats never became rows at all; the pin-write authorization for that
        # migration was withdrawn over exactly this miscount. So: no anchor_cwd recorded
        # probes the one conventional path before concluding anything, a hit reads
        # normally (via `probed_anchor_cwd`, kept separate from `anchor_cwd` so a reader
        # always sees what the graph actually recorded vs what this function found by
        # convention); a miss is `unknown-office`,
        # never `no-office`, even the probe failing only means neither of the two paths
        # this function knows to check found one, not that no office exists anywhere.
        probed_anchor = None
        if anchor is None and facts["handle"]:
            candidate = str(_default_office_root() / facts["handle"].lower())
            if _dir_exists(candidate):
                probed_anchor = candidate
        effective_anchor = anchor or probed_anchor
        pin = read_project_pin(effective_anchor)
        if pin.error:
            pin_state = "unreadable"
        elif effective_anchor is None:
            pin_state = "unknown-office"
        elif pin.path and pin.value is None:
            pin_state = "unset"
        elif pin.value:
            pin_state = "declared"
        else:
            pin_state = "no-pin"
        triage_bucket = None
        duplicate_siblings = None
        name_resolution = None
        if pin_state == "declared" and pin.value:
            triage_bucket = bucket_map.get(f"repo:{pin.value}", "no-such-project")
            if triage_bucket == "duplicate_suspect":
                duplicate_siblings = await _duplicate_suspect_siblings(
                    pool, f"repo:{pin.value}")
            elif triage_bucket == "no-such-project":
                name_resolution = await _pin_name_resolution_note(pool, pin.value)
        live_cwd = None
        if occ["state"] == "occupied" and occ["holder"]:
            live_cwd = await pool.fetchval(
                "SELECT cwd FROM agent_mounts WHERE agent_id=$1 "
                "ORDER BY last_seen DESC LIMIT 1", occ["holder"])
        # pin_charter_agreement: a mechanism that works (this row already carries both
        # `pin.declared` and `chartered_repos`) once fed a reader that never compared
        # them, so a mis-binding surfaced only when someone happened to notice and say
        # something. A mark, never a verdict: "agree"/"disagree" only, never "this one
        # is right". pin.declared is not certified canonical (this function's own
        # long-standing caveat, unchanged), so a disagreement names a conflict for a
        # person or agent to resolve with rebind_seat/correct_pin_value/set_charter,
        # never auto-picked here. "n/a" (not "disagree") when there is nothing to
        # compare: an unset/unreadable/unknown-office pin is a valid state, not a
        # defect, and an uncharted seat has no charter to disagree with. Both are
        # silence, not conflict.
        if pin_state != "declared" or not chartered:
            pin_charter_agreement = "n/a"
        elif pin.value in chartered:
            pin_charter_agreement = "agree"
        else:
            pin_charter_agreement = "disagree"
        # Presentation, never the graph: `chartered_repos` stays raw canonicals for
        # `pin_charter_agreement`'s own comparisons (which must keep operating in
        # canonical space forever), while `chartered_repos_display` adds the
        # name-with-canonical rendering a human reading roster text actually wants,
        # resolved live so a rename shows through immediately.
        from src.orchestrator.project_identity import charter_display_labels

        chartered_display = await charter_display_labels(pool, chartered)
        rows.append({
            "seat": seat_id, "handle": facts["handle"], "house": facts["house"],
            "manager": manager_handle,
            "occupancy": occ["state"], "holder": occ["holder"],
            "anchor_cwd": anchor, "tree_cwd": facts["tree_cwd"], "live_cwd": live_cwd,
            "probed_anchor_cwd": probed_anchor,
            "office_exists": _dir_exists(effective_anchor),
            "chartered_repos": chartered,
            "chartered_repos_display": chartered_display,
            "pin_charter_agreement": pin_charter_agreement,
            "pin": {"declared": pin.value, "state": pin_state, "path": pin.path,
                    "error": pin.error, "triage_bucket": triage_bucket,
                    **({"duplicate_siblings": duplicate_siblings}
                       if duplicate_siblings is not None else {}),
                    **({"name_resolution": name_resolution}
                       if name_resolution is not None else {})},
        })

    if repo is None:
        return {"seats": rows, **_roster_caveats_out(list(_ROSTER_CAVEATS), want_caveats)}

    name = repo.removeprefix("repo:").strip()
    matches = [
        {"seat": r["seat"], "handle": r["handle"], "occupancy": r["occupancy"],
         "holder": r["holder"],
         "via": [v for v, hit in (("charter", name in r["chartered_repos"]),
                                  ("pin", r["pin"]["declared"] == name)) if hit]}
        for r in rows
        if name in r["chartered_repos"] or r["pin"]["declared"] == name
    ]
    manager: str | None = None
    if not matches:
        agreement = "no-match"
    elif len(matches) == 1:
        agreement = "single-match"
    elif len(matches) == 2:
        charter_seat = next((m["seat"] for m in matches if "charter" in m["via"]), None)
        pin_seat = next((m["seat"] for m in matches if "pin" in m["via"]), None)
        agreement = "conflict"
        if charter_seat and pin_seat and charter_seat != pin_seat:
            if await manager_of_seat(pool, pin_seat) == charter_seat:
                agreement = "governed"
    else:
        agreement = "conflict"
        # Shared-house: a query matching an entire house's worth of seats against their
        # own house repo once read as a several-way "conflict", two or more seats
        # fighting over one repo, when it's actually the normal shape of a house's own
        # home repo: every worker legitimately charters/pins it, and the house's manager
        # already governs it. N>2 matches, all belonging to the same derived house, and
        # that house's name is this repo, is that shape, never guessed at for anything
        # else (a repo an unrelated cluster of seats merely happens to share stays a
        # plain conflict).
        matched_rows = {m["seat"]: r for m in matches for r in rows if r["seat"] == m["seat"]}
        houses = {r["house"] for r in matched_rows.values()}
        if len(houses) == 1:
            (house,) = houses
            if house and house.lower() == name.lower():
                for r in rows:
                    if (r["house"] and r["house"].lower() == name.lower()
                            and await manager_of_seat(pool, r["seat"]) is None):
                        agreement = "shared-house"
                        manager = r["seat"]
                        break

    near_misses: list[dict[str, Any]] = []
    caveats = list(_ROSTER_CAVEATS)
    if agreement == "no-match":
        caveats.append(
            "no-match means neither a seat's charter nor its current pin names this repo, "
            "not that the repo has no owner. It may be owned by a seat whose pin this "
            "function cannot read, chartered under a name this repo string doesn't exactly "
            "match, or simply not yet declared anywhere this function looks.")

        def _norm(s: str) -> str:
            return re.sub(r"[^a-z0-9]", "", s.lower())

        target = _norm(name)
        found: dict[tuple[str, str], set[str]] = {}
        for r in rows:
            candidates = [("charter", c) for c in r["chartered_repos"]]
            if r["pin"]["declared"]:
                candidates.append(("pin", r["pin"]["declared"]))
            for via, candidate in candidates:
                if candidate != name and _norm(candidate) == target:
                    found.setdefault((r["seat"], candidate), set()).add(via)
        for (seat, candidate), vias in sorted(found.items()):
            differs = "case" if candidate.lower() == name.lower() else "separator"
            near_misses.append({"repo": candidate, "seat": seat, "via": sorted(vias),
                                 "differs_by": differs})

    return {"repo": name, "matches": matches, "agreement": agreement,
            **_roster_caveats_out(caveats, want_caveats),
            "near_misses": near_misses, "manager": manager}


# A case-folded deny-list, not a detector: these are project-string values found in a
# fresh agent-project-distribution sweep that look like deliberate test/security-
# research fixtures, not real fleet work. A phantom_verdict must name this class
# explicitly rather than either scoring it a false phantom or silently dropping it from
# the report.
_KNOWN_TEST_FIXTURE_PROJECTS = frozenset({
    "cc-test-auto", "v218-evil-repo", "rce-disclosure-kit", "cc-clean", "rc-test",
    "nonexistent-probe", "smoketest", "resume_probe",
})

# The operationalization of "name-shape": the underlying test is eyeballing whether a
# project's name reads as a directory basename (generic, filesystem-y) versus a
# deliberately chosen one, a call made by hand against the actual originating cwd. That
# cwd is usually long gone by the time a phantom is this report's business
# (agent_mounts is a live/recent registry, not history: measured live, several dozen
# distinct cwds today against thousands of historical agents), so the only honest
# mechanical stand-in left is this explicit, editable list of generic path-segment
# names. It will always be incomplete: a reader who disagrees with an entry, or is
# missing one, is disagreeing with this list, not with some hidden ground truth. That is
# by design, since phantom status is a judgment call, and this makes it one a reader can
# see and contest.
_GENERIC_PATH_BASENAMES = frozenset({
    "seats", "code", "tmp", "temp", "src", "repo", "repos", "workspace", "projects",
    "project", "worktrees", "worktree", "container", "root", "home", "office", "scratch",
})

_TREE_LEDGER_CAVEATS = (
    "project_ledger covers every ACTIVE SoftwareProject (58 today, measured), durable "
    "graph state, but by construction can only see a phantom that accumulated at least "
    "one works_in edge; a tree that mounted, produced nothing, and left is invisible to "
    "this whole class of instrument, not just this one field.",
    "live_cwd_ledger's own non-durability caveat (agent_mounts is a live/recent registry, "
    "not history) is stated in ITS OWN section, not only here (nobody reads the bottom); "
    "see its `note` field, not just this list.",
    "phantom_verdict is a JUDGMENT, not a proof: "
    "'declared' trusts any seat's pin OR a Seat-origin governs edge, never an Agent-"
    "origin one (a known succession-leak class is evidence FOR phantom, not "
    "against it). project_ledger's own `phantom_verdict_basis`/`note` fields carry the "
    "editable basename list and the mechanical-vs-hand-judgment distinction directly, see "
    "those, not just this line.",
    "the pin's own seat/house/kind fields (a later schema addition) read "
    "almost universally absent as of this build: the migration/writer is still landing. "
    "That is the honest starting state, not a defect in this report's "
    "first pass; nothing here consumes those fields for a verdict yet, matching the same "
    "note that nothing else does either.",
    "READ-ONLY: this reports disagreements and phantom suspicions, it never repairs, folds, "
    "or merges anything; repo:code's disposition is a separate design decision, not this verb's.",
    "A GAP IN THE CANONICAL READER ITSELF, FLAGGED NOT FIXED: "
    "`_read_osiris_key` (agents.py, used unmodified by this report and by every other "
    "caller, roster(), resolve_identity, project_identity_evidence) climbs `cwd` and its "
    "parents for `.osiris` but never checks whether `cwd` ITSELF exists, so a query against "
    "a DELETED directory silently returns whatever an ancestor's pin says instead. This "
    "instrument works around it locally (`directory_exists`, checked before trusting the "
    "pin) but the canonical reader's own three-state model (OsirisKeyRead) is still missing "
    "a fourth state for this, everywhere else it's called too.",
)


async def _declared_project_index(pool: asyncpg.Pool) -> dict[str, list[str]]:
    """Every project name any active seat's pin or Seat-origin `governs` edge (charter_of,
    already Seat-origin-only: an Agent-origin governs edge never reaches this index)
    currently declares, keyed to the seat(s) declaring it. The fleet-wide half of the
    phantom test: a candidate is legitimate if some seat's own declaration claims it, not
    only the tree currently under judgment (the calibration case: a basename-shaped
    handle made legitimate purely by its own deliberate pin). Reuses roster() rather
    than re-querying pins/charters a second way."""
    ros = await roster(pool)
    index: dict[str, list[str]] = {}
    for row in ros["seats"]:
        for name in row["chartered_repos"]:
            index.setdefault(name, []).append(row["seat"])
        if row["pin"]["state"] == "declared" and row["pin"]["declared"]:
            index.setdefault(row["pin"]["declared"], []).append(row["seat"])
    return index


def _phantom_verdict(name: str, declared_by: list[str]) -> str:
    """In order: a known test fixture is named as one, never silently scored either way;
    a project any seat's own declaration claims is `declared` regardless of how generic
    its name looks (pin/charter overrides name-shape, not the other way around, per the
    calibration case above); failing that, a name matching `_GENERIC_PATH_BASENAMES` is
    a `phantom-suspect`; anything else is `undetermined`, a real disagreement worth a
    human's eye, not a confident phantom call either way."""
    if name.lower() in _KNOWN_TEST_FIXTURE_PROJECTS:
        return "test-fixture"
    if declared_by:
        return "declared"
    if name.lower() in _GENERIC_PATH_BASENAMES:
        return "phantom-suspect"
    return "undetermined"


async def project_ledger(pool: asyncpg.Pool, *, limit: int = 200, offset: int = 0,
                         ) -> dict[str, Any]:
    """The durable half of the pin-vs-graph disagreement report: every active
    SoftwareProject, cross-checked against the fleet-wide declared-name index
    (`_declared_project_index`) and given a `phantom_verdict` (`_phantom_verdict`),
    never a repair, only a report. `triage_bucket` is reused verbatim from the same
    `_triage_bucket_map` roster()'s pin field already calls, so a project's health reads
    identically whether reached through a seat's pin or through this fleet-wide sweep.

    `limit`/`offset` (default 200/0, capped 2000, `total` always reported): the
    no-silent-caps law, though 58 active SoftwareProjects exist today (measured),
    comfortably inside one page.

    `phantom_verdict_basis` and `note` are returned with the rows, not left to a
    docstring a caller may never read: `phantom-suspect` is a mechanical, weaker
    stand-in for a hand-verified name-shape judgment that looks at the actual
    originating cwd; this function cannot do that, because agent_mounts evicts it long
    before a phantom accumulates enough content to be this report's business (see
    `live_cwd_ledger`). A reader must be able to tell a mechanically-derived suspicion
    from a hand-verified one at a glance, in the data, not by tracing back to this
    function's source."""
    limit = max(1, min(limit, 2000))
    offset = max(0, offset)
    total = await pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' AND status='active'")
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS name "
        "FROM objects o WHERE o.type='SoftwareProject' AND o.status='active' "
        "ORDER BY o.canonical LIMIT $1 OFFSET $2", limit, offset)
    bucket_map = await _triage_bucket_map(pool)
    declared_index = await _declared_project_index(pool)
    projects: list[dict[str, Any]] = []
    for r in rows:
        name = r["name"] or r["canonical"].removeprefix("repo:")
        agent_count = await pool.fetchval(
            "SELECT count(DISTINCT l.from_id) FROM links l WHERE l.to_id=$1 "
            "AND l.type='works_in' AND (l.valid_until IS NULL OR l.valid_until > now())",
            r["id"])
        declared_by = sorted(set(declared_index.get(name, [])))
        projects.append({
            "project": r["canonical"], "name": name,
            "triage_bucket": bucket_map.get(r["canonical"]),
            "agent_count": agent_count,
            "declared_by": declared_by,
            "phantom_verdict": _phantom_verdict(name, declared_by),
        })
    return {
        "projects": projects, "total": total, "limit": limit, "offset": offset,
        "phantom_verdict_basis": {
            "test-fixture": sorted(_KNOWN_TEST_FIXTURE_PROJECTS),
            "phantom-suspect": sorted(_GENERIC_PATH_BASENAMES),
        },
        "note": ("phantom-suspect is MECHANICAL and WEAKER than a hand-verified judgment: "
                "it fires only when a project's name matches the editable list above AND "
                "no seat declares it. It is an operationalization of a "
                "name-shape test that survives the originating cwd being long "
                "gone, not a replacement for looking at one directly. undetermined "
                "means neither test fired, a real disagreement for a human, never a "
                "confirmation the project is legitimate. `declared` IS ALSO NOT A REALNESS "
                "CHECK (confirmed by live specimens climintworker1/"
                "inferredworker1's own pins declaring cliproj1/soleseathouse, debugging "
                "artifacts, correctly self-declared): this bucket answers 'does some seat's "
                "own declaration claim this name', never 'is this a genuine, intentional "
                "project'. A fake project that correctly declares itself is "
                "indistinguishable from a real one by declaration alone."),
    }


async def live_cwd_ledger(pool: asyncpg.Pool) -> dict[str, Any]:
    """The live half of the pin-vs-graph disagreement report: every distinct `cwd`
    `agent_mounts` holds right now (measured live: several dozen rows today, an
    ephemeral, recent-sessions registry, never a historical ledger; see
    `_TREE_LEDGER_CAVEATS`) cross-checked against what a fresh mount would resolve there
    today (`resolved_today`: the declared pin, else the basename fallback, refusing at
    the bare seats container exactly as `resolve_project` already does) versus what the
    graph currently believes (`graph_believes`: live `works_in` targets of every agent
    this cwd's rows name).

    `resolved_today` answers one specific question, not "what will this cwd's occupant
    see next": it is the cold/bootstrap resolution (pin, else basename) only. A seated
    agent's real mount() resolves seat-first (`_seated_house`/`derive_house`) and never
    consults this path at all (roster()'s own docstring: "a seated agent's project is
    its seat's derived house, unconditionally"). This field answers "what would an
    unknown agent resolve to here right now", not a prediction for whoever already
    lives there.

    `directory_exists` is checked first, before the pin is trusted at all: `_read_
    osiris_key` (the canonical reader, used unmodified) climbs `cwd` and every one of
    `cwd.parents` looking for `.osiris`, but never checks whether `cwd` itself exists.
    For a deleted office (two real, now-retired seats whose directories are gone were
    the specimens that surfaced this), the climb silently lands on the enclosing
    seats-container's own pin and reports it as if it belonged to the deleted office,
    collapsing "this office is gone" into "this office exists, pin unset," two
    conditions with opposite dispositions (one wants a pin written, the other wants the
    graph's belief reaped). This is a real gap in the canonical reader itself, not only
    this call site; it was flagged rather than silently patched here, and
    `_read_osiris_key` is unchanged. What is fixed here is local: `directory_exists=False`
    short-circuits `pin_state` to `missing-directory` before the climb's result is
    trusted for anything, and `resolved_today` is forced to `None`, since "a fresh mount
    would basename-fallback here" is meaningless when nothing can even `cd` there.

    `agreement`, six states now, not five: `ghost` is new: `directory_exists=False` and
    the graph still believes something. The office is gone, but nothing reconciles the
    graph's belief, a worse, different finding than a live misresolution risk, not a
    variant of one. Distinct from `graph-only` (the bare seats container itself, which
    is real on disk, `directory_exists=True`, deliberately refusing resolution by
    design). The other five, unchanged: `no-graph-yet` (nothing to cross-check, benign,
    covers a ghost with no belief either, since there's nothing to reconcile),
    `graph-only`, `match`, `partial-match` (today's resolution is one of the graph's
    beliefs, but the graph also carries other, likely-stale beliefs, worth a look, not
    urgent), `mismatch` (today's resolution matches none of the graph's beliefs while
    the directory is real, the live misresolution risk)."""
    rows = await pool.fetch(
        "SELECT cwd, count(DISTINCT agent_id) AS n_agents, "
        "array_agg(DISTINCT agent_id) AS agents FROM agent_mounts GROUP BY cwd "
        "ORDER BY cwd")
    from src.orchestrator.agents import read_project_pin
    from src.orchestrator.offices import is_bare_office_root

    cwds: list[dict[str, Any]] = []
    for r in rows:
        cwd = r["cwd"]
        directory_exists = _dir_exists(cwd)
        pin = read_project_pin(cwd)
        if not directory_exists:
            pin_state = "missing-directory"
        elif pin.error:
            pin_state = "unreadable"
        elif pin.path and pin.value is None:
            pin_state = "unset"
        elif pin.value:
            pin_state = "declared"
        else:
            pin_state = "no-pin"
        if not directory_exists:
            resolved_today: str | None = None
        elif pin.value:
            resolved_today = pin.value
        elif is_bare_office_root(cwd):
            resolved_today = None
        else:
            resolved_today = Path(cwd).name or None
        believes_rows = await pool.fetch(
            "SELECT DISTINCT p.canonical FROM objects a "
            "JOIN links l ON l.from_id=a.id AND l.type='works_in' "
            "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
            "WHERE a.canonical = ANY($1::text[])", list(r["agents"]))
        graph_believes = sorted(row["canonical"].removeprefix("repo:") for row in believes_rows)
        if not graph_believes:
            agreement = "no-graph-yet"
        elif not directory_exists:
            agreement = "ghost"
        elif resolved_today is None:
            agreement = "graph-only"
        elif resolved_today not in graph_believes:
            agreement = "mismatch"
        elif len(graph_believes) == 1:
            agreement = "match"
        else:
            agreement = "partial-match"
        cwds.append({
            "cwd": cwd, "agents_mounted": r["n_agents"], "directory_exists": directory_exists,
            "pin": {"declared": pin.value, "state": pin_state, "path": pin.path,
                    "error": pin.error},
            "resolved_today": resolved_today,
            "graph_believes": graph_believes,
            "agreement": agreement,
        })
    return {
        "cwds": cwds, "total": len(cwds),
        "note": ("NOT A HISTORICAL LEDGER (stated here, where a reader hits it, not at "
                "the bottom): this section's population is "
                "TODAY's agent_mounts table only, a live/recent registry keyed on job_dir "
                "that EVICTS old rows (measured live: 37 total rows / 32 distinct cwd "
                "against thousands of historical agents). A phantom whose originating "
                "sessions have already ended and been evicted from agent_mounts will NEVER "
                "appear here, only in project_ledger, which reads durable graph state "
                "instead."),
    }


async def tree_ledger(pool: asyncpg.Pool, *, limit: int = 200, offset: int = 0) -> dict[str, Any]:
    """The pin-vs-graph disagreement report: the instrument meant to catch phantom
    projects without a human having to notice by hand. Read-only, fleet-wide, two
    sections because the durable-history half and the live-right-now half genuinely
    need different populations (see each function's own docstring):

    `project_ledger`: every active SoftwareProject, judged against the fleet's own
    declared-name index, `phantom_verdict` named explicitly (a judgment, not a proof).
    `live_cwd_ledger`: every cwd `agent_mounts` holds right now, cross-checked against
    what a fresh mount resolves there today versus what the graph currently believes.

    `caveats` (`_TREE_LEDGER_CAVEATS`) is always returned alongside both, naming exactly
    what this instrument cannot see rather than reporting a clean sweep over a coverage
    boundary it never states. Never repairs: this names disagreements and phantom
    suspicions; disposing of one (a fold, a rename, a merge) is always a separate,
    deliberate, evidence-gated verb's job, never this one's."""
    projects = await project_ledger(pool, limit=limit, offset=offset)
    cwds = await live_cwd_ledger(pool)
    return {"project_ledger": projects, "live_cwd_ledger": cwds,
            "caveats": list(_TREE_LEDGER_CAVEATS)}


async def reachability(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """Can this lineage be reached right now: the truthful answer, consulted from the
    Claude harness daemon's own job state. A mail send-receipt can lie during a
    compaction seam ("no resumable session, never handed to a fresh successor") while
    the daemon's job_for on the same lineage has held a live, resumable job the whole
    time. A stale disk/DB snapshot infers liveness; job_for reads it from the one place
    that cannot lag the seam: the daemon owns the job, so it knows the instant a
    successor exists, before any mount row or transcript file catches up.

    Composes with seat_occupancy: occupancy answers "is a live body here" (holds link +
    agent_mounts); this answers "and can it receive a turn, this instant" (the daemon's
    own job table), two different authorities for two different questions, not a
    duplicate.

    Lineage-wide, matching every other liveness read in this codebase (held_seat,
    seat_occupancy, trigger.py's own `doors`): checks every job_dir this base or any of
    its generations has ever mounted, because a fresh successor's own mount row is
    exactly the evidence a seam can lag, since the daemon may already hold its job
    before agent_mounts reflects it at all.

    A read only, deliberately: this does not retry a refused send, notify anyone, or
    change what dispatch_dm does. It is the truthful primitive that any notify-at-seam
    work should consult, not a rewrite of either. `job_for` is a pure read of daemon job
    state (claude_daemon.job_for), never the `reply` injection lane, a distinction worth
    keeping even though that lane is also sanctioned: a read cannot move another mind,
    so this stays side-effect-free by construction, not by policy. Fails open like
    claude_daemon's own convention: a dark daemon or an unknown lineage reads
    unreachable-by-this-check, never treated as proof of death, only as "this read
    couldn't confirm it"."""
    from src.ingest.harness.claude_daemon import job_for
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT job_dir FROM agent_mounts WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') "
        "AND job_dir IS NOT NULL", base)
    doors = {Path(r["job_dir"]).name for r in rows if r["job_dir"]}
    if not doors:
        return {"reachable": False, "via": "none", "job": None,
                "detail": "no known job_dir for this lineage, nothing to ask the daemon "
                          "about"}
    ids = doors | {d[:8] for d in doors}
    job = await job_for(ids)
    if job is None:
        return {"reachable": False, "via": "none", "job": None,
                "detail": "the daemon holds no job for this lineage right now, dark or "
                          "genuinely not running; never treated as proof of death"}
    shown = job.get("short") or job.get("sessionId") or "its job"
    return {"reachable": True, "via": "daemon-job", "job": job,
            "detail": f"the daemon's own job state confirms {shown} is live right now"}


async def manager_of_seat(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """The manager Seat of a worker Seat, or None when unmanaged: the single-pair read
    originally promoted here so notify-at-seam (mint_heir's compaction path) didn't
    hand-roll a third copy. The stop hook's own former local `_manager_seat` duplicate
    is gone: the claim that it "cannot import across the script/package boundary
    without a heavier refactor" was stale, since the hook already puts the repo root on
    sys.path and imports `src.orchestrator.mailbox`/`.seats` directly
    (osiris_stophook.py's own `_resolve_worker_identity`); it now calls this function
    instead of its own copy."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT t.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical=$1 AND l.type='managed_by' "
        "AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)


async def seats_managed_by(pool: asyncpg.Pool, seat_id: str) -> list[str]:
    """The worker Seats `seat_id` itself manages: the reverse of `manager_of_seat`, which
    only ever answers worker to manager. Nothing here could previously ask "does this
    seat manage anyone" without walking the whole fleet by hand, and that question is
    the one non-inferred signal a manager-authored-code flag could ever use. Active
    `managed_by` edges only (never a retired or superseded one); an empty list means
    genuinely unmanaged-by, not merely un-checked, and is the correct read for "this is
    not a manager seat." Ordered by `first_seen` for a stable, reproducible listing; no
    house-derivation, no chain-walk, a single-hop reverse lookup, same shape as
    `manager_of_seat`'s own single-hop forward one."""
    rows = await pool.fetch(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 AND l.type='managed_by' "
        "AND f.type='Seat' AND f.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen ASC", seat_id)
    return [r["canonical"] for r in rows]


async def team_roster(
    pool: asyncpg.Pool, manager_seat_id: str, *, manager_house: str | None = None,
) -> list[dict[str, Any]]:
    """The `team` MCP tool's own core query, pulled out of mcp_server.py so the console
    command's own seat-argument path (a direct-DB console command, same shape as
    cmd_stop/cmd_correct_pin_value: it resolves a handle to a seat and calls the same
    logic, never a second copy) can call it without going through team()'s own
    deliberately self-scoped MCP contract. Every seat `managed_by` `manager_seat_id`:
    `live` (the one liveness authority, `mounts.agent_liveness`, see below), `owe`/
    `stale` (open obligations owned by that seat's handle, same definition
    `owned_obligations` uses), `envelope` (that seat's current holder's own unread ask
    count, scoped to `manager_house`, 0 with no live holder). Empty list means manages
    nobody; the caller decides what that means.

    A prior liveness convergence fix: the exact-holder, mounts-only, non-lineage-widened
    `EXISTS` this query used to run inline disagreed with `agent_liveness` in two ways at
    once: no lineage-base widening (a promoted successor generation's own mount row
    wears a different numeral than the `holds` edge's exact `holder` canonical, reading
    cold here while `agent_liveness` correctly followed the lineage) and no
    transcript-mtime fallback (a mind live and writing, with no fresh `agent_mounts` row
    to say so). The `live_secs`-scoped `EXISTS` is gone entirely, `agent_liveness` owns
    its own window, so the verdict now comes from the same single source `resume`'s
    occupancy gate and `vacate_dead_seat` also call: one instrument, not three
    disagreeing ones."""
    rows = await pool.fetch(
        "SELECT s.canonical AS seat, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=s.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle, "
        "  h.holder AS holder "
        "FROM links mb JOIN objects s ON s.id=mb.from_id "
        "JOIN objects mgr ON mgr.id=mb.to_id "
        "LEFT JOIN LATERAL ("
        "  SELECT a.canonical AS holder FROM links hl JOIN objects a ON a.id=hl.from_id "
        "  WHERE hl.to_id=s.id AND hl.type='holds' "
        "    AND (hl.valid_until IS NULL OR hl.valid_until > now()) "
        "  ORDER BY hl.created_at DESC LIMIT 1"
        ") h ON true "
        "WHERE mgr.canonical=$1::text AND mb.type='managed_by' AND s.status='active' "
        "  AND (mb.valid_until IS NULL OR mb.valid_until > now()) "
        "ORDER BY handle ASC",
        manager_seat_id)
    from src.orchestrator.mailbox import unread_counts
    from src.orchestrator.mounts import agent_liveness
    from src.orchestrator.stophook_logic import owned_obligations

    out_rows: list[dict[str, Any]] = []
    for r in rows:
        obl = await owned_obligations(pool, r["handle"] or r["seat"])
        envelope = 0
        live = False
        if r["holder"]:
            counts = await unread_counts(pool, manager_house or "", reader_agent=r["holder"])
            envelope = counts["ask"]
            live = (await agent_liveness(pool, r["holder"]))["live"]
        out_rows.append({
            "handle": r["handle"], "live": live, "owe": obl["owned"],
            "stale": obl["stale"], "envelope": envelope,
        })
    return out_rows


async def _managed_by_source(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """The `managed_by` edge's own source_id for `seat_id`'s active manager link, or None
    when unmanaged: a second read alongside manager_of_seat's (same row) so derive_house
    can tell who authorized this specific management relationship, distinct from the bare
    fact of it. Not folded into manager_of_seat itself: that helper has five existing
    callers, all treating its return as a bare str | None, so widening it would ripple to
    every one."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT l.source_id FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical=$1 AND l.type='managed_by' "
        "AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)


async def _own_house_stamp(pool: asyncpg.Pool, seat_id: str) -> tuple[str | None, str | None]:
    """(house value, its own source_id) for `seat_id`'s own stored `house` property: one
    read reused both when `seat_id` is a genuine head (no manager at all) and when it's an
    operator-crossed anchor (managed, but the crossing itself keeps its own house)."""
    row = await pool.fetchrow(
        "SELECT a.value #>> '{}' AS house, a.source_id FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='house' "
        "WHERE o.canonical=$1 ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        seat_id)
    return (row["house"], row["source_id"]) if row else (None, None)


async def _is_ghost_house(pool: asyncpg.Pool, seat_id: str, house: str) -> bool:
    """The ghost clause: a seat's stamped `house` equal to its own governed project's
    name, case-insensitive, is a leftover from before houses were optional at all, the
    era when a seat's house and its charter were the same string by construction, never
    a deliberate house declaration. `derive_house` must read it as empty before any
    comparison, exactly as if the property had never been stamped, on both branches that
    consult it (a head's own answer, and the anchor's real-crossing check alike), never a
    special case one of the two forgets. Compares against each governed project's own
    current name, not charter_of's raw canonical: a house correctly re-stamped to a
    project's live name by resync_seat_project must still ghost-match once that
    project's own name has since moved again, the same "compare by what it means today"
    discipline every other part of this derivation holds."""
    from src.orchestrator.charter import charter_of, project_current_name

    governed = await charter_of(pool, seat_id)
    names = [await project_current_name(pool, g) for g in governed]
    return any(house.lower() == n.lower() for n in names)


_MAX_HOUSE_HOPS = 32  # Generous for any real org depth (mirrors mint_heir's own bounded-
                      # walk convention, range(64)). Exists to catch a managed_by cycle,
                      # a data bug, not a legitimately deep chain.


async def derive_house(pool: asyncpg.Pool, seat_id: str, *, max_hops: int = _MAX_HOUSE_HOPS,
                       ) -> str | None:
    """House must be derived, not stored: house(seat) = house(manager(seat)), walked up
    the managed_by chain to the head (a seat with no active managed_by edge out). The
    head's own stored house is the one legitimate anchor left, a deliberate declaration
    never a spawn-time snapshot. Every seat below derives through the chain; the stored
    `house` property on a non-head seat is legacy noise this never reads (an old,
    orphaned house stamp on a demoted seat simply stops being consulted, not corrected).

    The house anchor: a cross-house adoption bug once silently annexed one house's seats
    into another and, escalated, leaked a batch of messages into the wrong seat's
    mailbox. A managed_by edge the operator's own hand asserted crosses a house boundary
    deliberately; mintseat's own cross-house-mint guard already refuses that crossing for
    anyone else. The walk stops at a seat only when both signals agree it is a genuine
    boundary crossing, never either alone:
      (1) a real crossing: this seat's own stamped `house` is non-empty and differs from
          what deriving one hop further (through its manager) would answer. Required
          because the operator's hand alone is not enough: the specimen that forced this
          is the promotion verb's own operator-tab call shape, a freshly-promoted worker
          with no stamped house of its own has nothing to cross, so an operator-sourced
          managed_by edge onto it must still derive the new manager's house, not anchor at
          nothing.
      (2) the operator's own hand on the crossing: either the managed_by link to its
          manager was itself asserted by an operator actor (`_OPERATOR_ACTORS`, the
          live, empirically-verified signal: an adoption event stamps the link with
          source='operator' even when it never re-touches an already-existing seat's
          own `house` property, which is exactly what happened in one specimen, a
          seat's house property still carried the source from its original mint, an
          unrelated agent id, so checking only the property's own source would silently
          miss the seat that actually caused a mail-routing breach) or the seat's own
          `house` property was itself asserted by an operator actor (freshly
          operator-stamped at mint). Required because a real value difference alone is
          not enough either: a seat can carry an old, unrelated, never-corrected house
          stamp that differs from its current manager's chain for no deliberate reason
          at all: the operator's hand is what tells the two apart from a genuine
          annexation.
    Both signals present makes this seat a house anchor, treated as a head for house
    purposes even while it remains managed: management and habitation are different
    facts; the org chart may cross a boundary without annexing what it crosses.

    Ordinary derivation is unchanged for every seat missing either signal: a stray old
    stamp with no operator's hand on it still gets ignored exactly as always, and an
    operator-sourced edge onto a seat with no stamped house of its own still derives
    through, never anchoring at emptiness.

    The ghost clause: a managed seat's own stamped `house` equal to its own governed
    project's name, case-insensitive, is a ghost from before houses were optional, the
    era a seat's house and its charter were the same string by construction, never a
    deliberate declaration. `_is_ghost_house` reads it as empty before the anchor's
    real-crossing comparison runs, so a ghost never anchors (nothing crosses when there
    was never a real house there): it walks through to the manager's derivation exactly
    like a seat with no stamp at all. Deliberately blind on a head: the identical string
    match is the ordinary, legitimate shape for a head (a house governing the project of
    the same name), so the ghost clause only ever protects the managed-seat anchor
    check, never a head's own authoritative declaration.

    The none clause: a boundary needs two real, different houses. A manager side that
    itself derives to None never anchors the seat below it, however loud the operator's
    hand on the crossing. A promoted worker whose stamped house differs from a manager
    who is itself houseless (its own chain never reaches a real stamp) walks through to
    that None rather than anchoring at its own now-orphaned value: the anchor exists to
    preserve a real house crossing a real boundary, not to manufacture one where the far
    side never had a house to cross into.

    Read-time only, same discipline as reachability(): computed fresh every call,
    nothing written back. Loud on a managed_by cycle: a seat reappearing in its own
    chain is a graph bug, not a deep hierarchy, so this logs and returns None rather
    than silently truncating; a legitimately unbounded chain (should never happen, since
    max_hops is generous) reads the same way, also logged."""
    seen: set[str] = set()
    current = seat_id
    for _ in range(max_hops):
        if current in seen:
            logger.warning("managed_by cycle deriving house for %s: reached %s twice",
                           seat_id, current)
            return None
        seen.add(current)
        manager = await manager_of_seat(pool, current)
        if manager is None:  # current is the head: its own stamped house is authoritative,
            # ghost-blind on purpose. A head's own house deliberately matching its own
            # flagship repo (a house governing the project of the same name) is the
            # common, legitimate case, not a leftover. The ghost clause only ever
            # protects the anchor comparison below from a managed seat's stale pre-house
            # snapshot, never a head's own deliberate declaration.
            house, _source = await _own_house_stamp(pool, current)
            return house
        house, house_source = await _own_house_stamp(pool, current)
        if house and await _is_ghost_house(pool, current, house):
            house = None
        if house:
            link_source = await _managed_by_source(pool, current)
            # Reviewed and left alone: this asks whether a historical write was stamped
            # by the operator's hand, a read-only provenance signal for house-boundary
            # detection, not a live "is this actor allowed to do X" gate, and there is
            # no project here to scope a charter check against.
            if link_source in _OPERATOR_ACTORS or house_source in _OPERATOR_ACTORS:
                manager_house = await derive_house(pool, manager, max_hops=max_hops)
                if manager_house is not None and house != manager_house:
                    return house  # A house anchor: a real crossing, operator's hand.
        current = manager
    logger.warning("house derivation for %s exceeded %d hops without reaching a head",
                   seat_id, max_hops)
    return None


async def seat_facts(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any]:
    """A Seat's own handle/house/intended_model/anchor_cwd/tree_cwd, one read: the shared
    resolver mintseat.py and trigger.py each independently hand-rolled as a private
    `_seat_facts` (identical name, near-identical shape, the exact "two resolvers
    disagree" class this codebase keeps re-learning). `house` is derived, the other four
    are the seat's own stored assertions; always all five keys present (None when
    absent or the seat doesn't exist), matching trigger.py's stricter contract so a
    caller can index `facts["handle"]` directly rather than every caller re-deriving its
    own tolerant `.get()`.

    `tree_cwd` is distinct from `anchor_cwd` on purpose: the office is where identity
    lives, the tree is where code lives. See `bind_seat_tree`. None here means "this
    seat has no distinct tree" (the common case), never a fallback guess; `launch_seat`
    is the one that decides what None means for a launch."""
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
        "   AS anchor_cwd, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='tree_cwd' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS tree_cwd "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Seat' AND o.status='active'", seat_id)
    house = await derive_house(pool, seat_id)
    if row is None:
        return {"handle": None, "house": house, "intended_model": None, "anchor_cwd": None,
                "tree_cwd": None}
    return {"handle": row["handle"], "house": house,
            "intended_model": row["intended_model"], "anchor_cwd": row["anchor_cwd"],
            "tree_cwd": row["tree_cwd"]}


async def backfill_anchor_cwd_from_live_observation(
    actions: Actions, *, actor: str, live_secs: int = _LIVE_SECS,
) -> dict[str, Any]:
    """A seat that is occupied right now but has never had its `anchor_cwd` captured has
    no durable trace of its office at all: the moment it goes cold, the location is gone
    from the graph, not merely undeclared. This stamps `anchor_cwd` from a live,
    first-hand observation the instant one exists, so that knowledge survives the seat
    going cold, the same observation the fleet-observer already records continuously for
    every other fact it touches (lineage.py), not a declaration on anyone else's behalf.
    A pin written into another house's office is a claim by someone else's hand,
    forbidden cross-house right now; an anchor_cwd stamped from what a live session is
    actually doing is an observation recorded, a different act, authorized here.

    Narrow by design:
    - Never overwrites. A seat with an existing `anchor_cwd` (any value, even stale) is
      skipped outright: there is nothing to arbitrate on this shape, only something
      missing, and a seat that already has an answer is not this function's business.
    - Occupied only. A cold or vacant seat has no live cwd to observe; nothing is guessed
      from history.
    - Ambiguous means nothing written. If the seat's live holder shows more than one
      distinct cwd across its own fresh `agent_mounts` rows (more than one concurrent
      session, disagreeing), this writes nothing for that seat: a missing value is
      recoverable on a later, cleaner observation; a wrong one is not.

    Returns `stamped` (seat -> the cwd it was given), `skipped_has_anchor`,
    `skipped_not_occupied`, `skipped_ambiguous`: every seat is accounted for in exactly
    one bucket, so a caller can see what happened to all of them, not just the
    successes."""
    seat_rows = await actions.pool.fetch(
        "SELECT o.canonical AS seat_id FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "ORDER BY o.canonical")
    stamped: dict[str, str] = {}
    skipped_has_anchor: list[str] = []
    skipped_not_occupied: list[str] = []
    skipped_ambiguous: list[str] = []
    now = datetime.now(UTC)
    for sr in seat_rows:
        seat_id = sr["seat_id"]
        facts = await seat_facts(actions.pool, seat_id)
        if facts["anchor_cwd"] is not None:
            skipped_has_anchor.append(seat_id)
            continue
        occ = await seat_occupancy(actions.pool, seat_id, live_secs=live_secs)
        if occ["state"] != "occupied" or not occ["holder"]:
            skipped_not_occupied.append(seat_id)
            continue
        cwd_rows = await actions.pool.fetch(
            "SELECT DISTINCT cwd FROM agent_mounts WHERE agent_id=$1 AND cwd IS NOT NULL "
            "AND last_seen > now() - make_interval(secs => $2)", occ["holder"], live_secs)
        distinct_cwds = {r["cwd"] for r in cwd_rows}
        if len(distinct_cwds) != 1:
            skipped_ambiguous.append(seat_id)
            continue
        cwd = next(iter(distinct_cwds))
        seat_oid = await actions.create_or_find_object("Seat", seat_id, actor)
        obs_ec = EvidenceClass.DIRECT_OBSERVATION
        await actions.assert_property(seat_oid, "anchor_cwd", cwd, actor, now,
                                      confidence_for(obs_ec), evidence_class=obs_ec.value)
        stamped[seat_id] = cwd
    return {"stamped": stamped, "skipped_has_anchor": skipped_has_anchor,
            "skipped_not_occupied": skipped_not_occupied,
            "skipped_ambiguous": skipped_ambiguous}


_ATTENDED_VALUES = {"human", "worker"}


async def set_seat_attended(
    actions: Actions, *, seat_id: str, attended: str, actor: str, because: str,
) -> dict[str, Any]:
    """Stamp a seat's real attendance signal: replaces a previous broken proxy in
    `dispatch_dm` ("a seat that manages someone is human-attended"), true only while a
    single manager sat at the top of the fleet and false the day workers started
    minting sub-workers and test seats of their own (a mint of test seats once
    reclassified their own creator this way; a batch of pilot workers did too, and both
    silently lost their push lane forever). `attended='human'` marks a seat the operator
    actually fronts; `attended='worker'` marks the ordinary case explicitly, for
    reversing a prior stamp. The human-attended guard reads this directly and no longer
    infers anything from managed_by.

    Operator-approved to change, enforced: this claimed it for weeks while any mounted
    caller could stamp any seat's attendance signal, the same unenforced-claim class as
    rename_seat, fixed the same way, mirroring charter_for's already-real check: `actor`
    must be one of `_OPERATOR_ACTORS`'s sentinels, or the seat `actor`'s own lineage
    holds must be the target seat's manager (`manager_of_seat`'s live `managed_by`
    edge).

    Refuses loudly on: a value outside {'human','worker'} (no silent typo landing as
    'not human'); a blank `because` (a safety guard reads this property, the reason it
    changed belongs on the record); an unauthorized actor; an unknown or retired seat (a
    Seat's `status` column stays 'active' forever; retirement is the `retired` property
    `retire_seat` stamps, the same signal checked here)."""
    if attended not in _ATTENDED_VALUES:
        return {"error": f"attended must be one of {sorted(_ATTENDED_VALUES)}, not "
                         f"{attended!r}"}
    if not because.strip():
        return {"error": "because is required: a seat's attendance signal gates a safety "
                         "guard (dispatch_dm's human-attended check); the reason it changed "
                         "must be on the record"}
    from src.orchestrator.charter import is_operator_actor

    if not await is_operator_actor(actions.pool, actor):
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        manager_seat_id = await manager_of_seat(actions.pool, seat_id)
        if caller_seat_id is None or caller_seat_id != manager_seat_id:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            manager_desc = manager_seat_id or "no manager on record"
            return {"error": f"{caller_desc} is not authorized to set attendance on "
                             f"{seat_id}: its manager is {manager_desc}, and {actor} is "
                             "neither the manager nor the operator"}
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    retired = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions a WHERE a.object_id=$1 AND a.name='retired' "
        "AND a.value #>> '{}' = 'true'", row["id"])
    if retired:
        return {"error": f"{seat_id} is retired, cannot stamp attendance on a retired seat"}
    # A status-gap fix: retire_seat now flips objects.status too, so a retired seat's
    # status is no longer 'active'. The lookup above must not filter on it up front, or
    # a retired seat reads as "no such seat" instead of the specific message above. A
    # merged seat (fold_seat) hits this same non-active branch, a pre-existing gap.
    if row["status"] != "active":
        return {"error": f"{seat_id} is {row['status']}, not active, nothing to stamp"}
    now = datetime.now(UTC)
    await actions.assert_property(row["id"], "attended", attended, actor, now, _CONF,
                                  evidence_class=_EC)
    return {"seat": seat_id, "attended": attended, "because": because}


async def rename_seat(
    actions: Actions, *, seat_id: str, new_handle: str, actor: str, because: str,
) -> dict[str, Any]:
    """Rename a seat, operator-ordered: no rename verb existed before this; claim_name is
    self-claiming only (a mind picks its own name), and handles across this fleet have
    drifted in casing (one handle lowercase at one layer, all-caps at another, for the
    same seat) with nothing to correct it deliberately. A manager or the operator
    renames a seat by hand, always with a reason on the record, enforced: this claimed
    "a manager or the operator" for weeks while any mounted caller could rename any
    seat; mirrors charter_for's already-real actor-vs-manager_of_seat check.

    Scope, both compensating assertions (old handle stays in history, never deleted):
    (1) the seat's own `handle` property; (2) the current holder's `handle` stamp too, if
    the seat is occupied, since a rename that only touched the seat would leave the live
    mind still answering to its old name in every seat_label() render. Mirrors
    claim_name's own 40-char cap on a handle: same class of field, same discipline.

    Out of scope, deliberately: the harness-session display name (a terminal/window
    title) is not touched: it belongs to a running process this verb has no reach into.
    The honest receipt is "graph renamed; the harness name follows at next spawn,"
    never a claim of something this call didn't do.

    Self-managed seats self-authorize: a seat with no manager on record is a legitimate
    shape, not an error state. The missing-manager refusal below used to fire
    unconditionally, even against that seat's own holder, leaving an unmanaged-and-cold
    seat with no possible actor at all except the literal operator sentinel. Mirrors
    bind_seat_tree's own carve-out exactly: a holder acting on its own seat, with no
    manager to defer to, is never a wider trust grant than the manager check already
    allows (a manager may rename any seat it manages; this only ever lets a holder
    rename its own). The receipt confesses it under `authorization` rather than reading
    identically to a manager-approved write.

    Refuses loudly on: a blank `new_handle` or one over 40 chars; a blank `because` (a
    rename is testimony, the reason must be on the record, the same discipline
    set_seat_attended holds); an unknown seat; `new_handle` already claimed by a
    different active seat, case-insensitive (seats_by_handle, the exact drift lesson
    that motivated this: two case variants of the same handle must never both be
    claimable); an unauthorized actor."""
    new_handle = (new_handle or "").strip()
    if not new_handle or len(new_handle) > 40:
        return {"error": "pick a short handle (1-40 chars)"}
    if not because.strip():
        return {"error": "because is required: a rename is testimony; the reason it "
                         "changed must be on the record"}
    self_authorized = False
    from src.orchestrator.charter import is_operator_actor

    if not await is_operator_actor(actions.pool, actor):
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        manager_seat_id = await manager_of_seat(actions.pool, seat_id)
        authorized = caller_seat_id is not None and caller_seat_id == manager_seat_id
        if not authorized and manager_seat_id is None and caller_seat_id == seat_id:
            authorized, self_authorized = True, True
        if not authorized:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            manager_desc = manager_seat_id or "no manager on record"
            return {"error": f"{caller_desc} is not authorized to rename {seat_id}: its "
                             f"manager is {manager_desc}, and {actor} is neither the "
                             "manager nor the operator"}
    row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    collisions = [s for s in await seats_by_handle(actions.pool, new_handle) if s != seat_id]
    if collisions:
        return {"error": f"'{new_handle}' is already claimed by {collisions[0]} "
                         "(case-insensitive), a name belongs to one seat forever"}
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
    out = {"seat": seat_id, "old_handle": old_handle, "new_handle": new_handle,
           "holder_stamped": holder_stamped, "because": because,
           "note": "graph renamed; the harness window/session display name follows at "
                   "next spawn, not retroactively"}
    if self_authorized:
        out["authorization"] = "self-authorized, no manager on record"
    return out


async def bind_seat_tree(
    actions: Actions, *, seat_id: str, tree_cwd: str, actor: str, because: str,
) -> dict[str, Any]:
    """Point a seat's code checkout at `tree_cwd`, deliberately distinct from
    `anchor_cwd` (the seat's identity home, untouched by this). Re-scoped so that "the
    office is where identity lives, the tree is where code lives", and collapsing the
    two once caused a real cross-checkout catastrophe repeated at seat scope.
    `launch_seat`'s own idempotency reuses whatever is recorded here across every
    relaunch until this is called again, never a launch's own side effect: which tree a
    seat is on stays an auditable graph write, same discipline `rename_seat` holds for a
    handle change.

    This system never provisions the tree (the harness owns isolation); this call
    records a location, it does not create one. `launch_seat` is the one that checks the
    directory actually exists on disk before trusting it; this verb writes
    unconditionally on a valid call, exactly as `ensure_seat`'s own `anchor_cwd` write
    does.

    Operator-or-manager only, enforced: this had no authority language at all, claimed
    or enforced, unlike its rename_seat/set_seat_attended siblings which at least
    overclaimed. Judged gate-worthy, not honesty-worthy, because the write is not merely
    descriptive metadata: `tree_cwd` is what `launch_seat` trusts as the code a
    relaunched seat executes, so a wrong or hostile rebind is a code-execution vector at
    the seat's next launch, not a cosmetic drift. Mirrors charter_for's already-real
    actor-vs-manager_of_seat check, the same pattern rename_seat/set_seat_attended now
    carry: `actor` must be one of `_OPERATOR_ACTORS`'s sentinels, or the seat `actor`'s
    own lineage holds must be the target seat's manager (`manager_of_seat`'s live
    `managed_by` edge), or, an unmanaged seat's own holder acting on its own seat only
    (the original check had no legal actor at all for a seat with no manager on record,
    not even that seat's own holder; a real specimen was once stranded on a dead
    tree_cwd after an operator-ordered folder move with nobody able to fix it). That
    carve-out never widens what a manager can already do (a manager may rebind any seat
    it manages; self-authorization only ever lets a holder rebind its own, and only when
    no manager exists to defer to instead). The receipt confesses it under
    `authorization` rather than reading identically to a manager-approved write.

    Refuses loudly on: a blank `tree_cwd`; a blank `because` (a location change is
    testimony, the same discipline `rename_seat`/`set_seat_attended` hold); an
    unauthorized actor; an unknown seat."""
    tree_cwd = (tree_cwd or "").strip()
    if not tree_cwd:
        return {"error": "bind_seat_tree needs a tree_cwd"}
    if not because.strip():
        return {"error": "because is required: a tree binding is testimony; the reason "
                         "it changed must be on the record"}
    self_authorized = False
    from src.orchestrator.charter import is_operator_actor

    if not await is_operator_actor(actions.pool, actor):
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        manager_seat_id = await manager_of_seat(actions.pool, seat_id)
        authorized = caller_seat_id is not None and caller_seat_id == manager_seat_id
        # Self-authorization, unmanaged only: a seat with no manager on record used to
        # have no legal actor at all for its own tree, not even its own holder, the
        # exact deadlock that once stranded a real seat on a dead on_disk_path after an
        # operator-ordered folder move nobody could correct. A holder acting on its own
        # seat, with no manager to defer to, is never a wider trust grant than the
        # manager check already allows (a manager may rebind any seat it manages; this
        # only ever lets a holder rebind its own), but it is still a bypass of the
        # normal chain, so the receipt confesses it explicitly rather than reading
        # identically to a manager-approved write.
        if not authorized and manager_seat_id is None and caller_seat_id == seat_id:
            authorized, self_authorized = True, True
        if not authorized:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            manager_desc = manager_seat_id or "no manager on record"
            return {"error": f"{caller_desc} is not authorized to bind {seat_id}'s tree: "
                             f"its manager is {manager_desc}, and {actor} is neither the "
                             "manager nor the operator"}
    row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    old_tree = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='tree_cwd' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        row["id"])
    # A seat-tree fabrication fix, operator-flagged: plain assert_property's own
    # supersession is same-source-only, so a manager's later, correct bind_seat_tree
    # call here never retired an earlier mint-time fabrication written by a different
    # source (e.g. "console"). Both sat simultaneously current, and launch trusted
    # whichever row current_assertions happened to return first. A live specimen: one
    # seat carried both a fabricated console-sourced path (from mint) and a real,
    # manually-corrected path from a later fix, and the real one silently never won.
    # tree_cwd is single-valued per seat regardless of who wrote it last;
    # assert_singular_property is this codebase's own entry point for exactly this shape,
    # collapsing every other current row, not just this source's own.
    await actions.assert_singular_property(
        row["id"], "tree_cwd", tree_cwd, actor, datetime.now(UTC), _CONF,
        because=f"{because} (bind_seat_tree: tree_cwd is single-valued per seat, "
                "cross-source collapse)",
        evidence_class=_EC)
    out = {"seat": seat_id, "old_tree_cwd": old_tree, "tree_cwd": tree_cwd,
           "because": because,
           "note": "recorded, osiris never provisions the directory itself; launch_seat "
                   "checks it exists before trusting it"}
    if self_authorized:
        out["authorization"] = "self-authorized, no manager on record"
    return out


async def sweep_seat_trees(
    actions: Actions, *, apply: bool = False, actor: str,
) -> dict[str, Any]:
    """The seat-tree fabrication sweep, operator-flagged: every active seat whose
    `tree_cwd` is null, or names a directory that is not a real git tree of its own
    governed project, listed (dry run) or rebound (`apply=True`), the fleet-wide
    cleanup for every specimen `found_seat`'s own mint-time fabrication already
    produced before its fix landed (this sweep never touches a seat minted after the
    fix, since those never fabricated a tree to begin with). Reuses `bind_seat_tree`
    unchanged for every real write (the same cross-source-safe collapse, never a second
    implementation), scoped by `actor` exactly as that verb already enforces (operator,
    or the target seat's own manager).

    A seat repairs when its own charter (`governed_trees`) names exactly one project
    with a recorded, real git tree: the unambiguous case. Every other shape is
    reported, never guessed: no tree_cwd problem at all (skipped, not listed), no
    charter (`refused-why: no charter`), more than one real governed tree
    (`refused-why: ambiguous charter`), or a charter whose own recorded path isn't
    actually a git tree either (`refused-why: no real governed tree`).

    A previously found collapse gap: the original read here was `ORDER BY ... LIMIT 1`,
    so a seat carrying two simultaneously-current tree_cwd rows (the exact disease this
    whole entry point exists to fix) was skipped whenever the LIMIT-1 read happened to
    surface the real one first, leaving its fabricated or duplicate sibling row current
    forever. Two real specimens: a real row sitting beside a fabricated console row, and
    the same real value asserted twice by two different sources. Fixed by reading every
    current row per seat: a seat with more than one current row is never skipped just
    because one of them happens to be real. Its real value (if exactly one distinct real
    value is present) is rewritten through `bind_seat_tree`, which collapses every
    other current row via `assert_singular_property` regardless of source, same as the
    charter-repair path already did."""
    from src.orchestrator.charter import governed_trees
    from src.orchestrator.trigger import _is_git_tree, _tree_exists

    rows = await actions.pool.fetch(
        "SELECT o.canonical AS seat_id, "
        "  array_remove(array_agg(a.value #>> '{}'), NULL) AS tree_cwds "
        "FROM objects o "
        "LEFT JOIN current_assertions a ON a.object_id=o.id AND a.name='tree_cwd' "
        "WHERE o.type='Seat' AND o.status='active' "
        "GROUP BY o.canonical")

    entries: list[dict[str, Any]] = []
    for r in rows:
        seat_id, tree_cwds = r["seat_id"], list(r["tree_cwds"] or [])
        distinct = sorted(set(tree_cwds))
        real_current = [v for v in distinct if _tree_exists(v) and _is_git_tree(v)]
        old_display: Any = tree_cwds[0] if len(tree_cwds) == 1 else (
            tree_cwds if tree_cwds else None)
        if len(tree_cwds) <= 1 and len(real_current) == 1:
            continue  # exactly one current row, and it's a real, bound tree
        if len(real_current) > 1:
            entries.append({
                "seat": seat_id, "old_tree_cwd": old_display, "new_tree_cwd": None,
                "refused_why": "ambiguous current values",
            })
            continue
        repo: str | None
        if len(real_current) == 1 and len(tree_cwds) > 1:
            # A real value is already present, just duplicated alongside a stale or
            # fabricated sibling row: collapse straight to it, no charter guess needed.
            winner, repo = real_current[0], None
        else:
            real_trees = [
                (rp, p) for rp, p in await governed_trees(actions.pool, seat_id)
                if _tree_exists(p) and _is_git_tree(p)]
            if len(real_trees) != 1:
                entries.append({
                    "seat": seat_id, "old_tree_cwd": old_display, "new_tree_cwd": None,
                    "refused_why": "no charter" if not real_trees else "ambiguous charter",
                })
                continue
            repo, winner = real_trees[0]
            # The current name, never the canonical: governed_trees' own repo label is
            # charter_of's frozen-at-mint canonical. This only ever feeds the
            # receipt/audit text below, never a graph write of its own (bind_seat_tree
            # writes `winner`, the tree path), but a stale label in an audit trail is
            # exactly the confusion this resolves.
            from src.orchestrator.charter import project_current_name
            repo = await project_current_name(actions.pool, repo)
        if not apply:
            entries.append({
                "seat": seat_id, "old_tree_cwd": old_display, "new_tree_cwd": winner,
                "refused_why": None, "repo": repo})
            continue
        because = (f"sweep_seat_trees: collapsed {len(tree_cwds)} current tree_cwd "
                   "rows onto its own already-real value" if repo is None else
                   f"sweep_seat_trees: repaired from its own charter ({repo})")
        bound = await bind_seat_tree(
            actions, seat_id=seat_id, tree_cwd=winner, actor=actor, because=because)
        entries.append({
            "seat": seat_id, "old_tree_cwd": old_display,
            "new_tree_cwd": bound.get("tree_cwd") if "error" not in bound else None,
            "refused_why": bound.get("error"), "repo": repo})
    return {
        "apply": apply, "swept": len(entries),
        "repaired": sum(1 for e in entries if e["new_tree_cwd"] and not e["refused_why"]),
        "refused": sum(1 for e in entries if e["refused_why"]),
        "entries": entries,
    }


async def bind_holder(
    actions: Actions, *, seat_id: str, agent_id: str, source: str | None = None,
) -> dict[str, Any]:
    """Make `agent_id` the seat's active holder: prior holders' `holds` links heal by
    valid_until (never deleted, history walkable), one active link remains. The shared
    tail of the two deliberate binding acts: the attach protocol (token-gated,
    spawner-driven) and a `claim_name` (guard-gated, the live mind's own act). Callers
    run their refusals first; this only writes.

    Now symmetric on both sides of `holds`: this always invalidated a seat's own prior
    holders before binding a new one, but never checked the agent's own other active
    `holds` edges elsewhere, so an agent could accumulate more than one live seat with
    nothing to catch or prevent it, the same add-only shape
    `works_in_added_alongside_prior` (agents.py) found and fixed for the sibling
    works_in relation. Unlike works_in, a different disposition applies here, not the
    same one copied over: works_in stayed additive-only because no evidence source can
    tell "changed project" from "works two projects" (a declared multi-project charter
    is a real, sanctioned state), so auto-invalidating there would be guessing exactly
    where that law forbids it. `holds` has no such ambiguity: a Seat is a specific
    identity ("one seat, one live lineage head" already establishes seat-holding as
    exclusive on the seat's own side; a visitor spawn is explicitly excluded from ever
    resolving as a holder at all, per binding_of_handle/seat_holder_ineligible's own
    spawned_by check) and no decision, spec, or charter concept anywhere in this
    codebase names a legitimate reason for one agent to hold two seats at once, so this
    widens the same already-trusted invalidate_link mechanism already used two lines
    below for the seat side, rather than adding a new additive-flag mechanism. Measured
    population before this fix: zero agents currently held more than one active seat
    (fleet-wide, not sampled). This closes the gap before an incident, the cheap time
    to close it.

    Returns a receipt: `{"seat_id", "old_holder", "new_holder"}`. `old_holder` is
    whoever this call just invalidated on the seat's own side (None if the seat was
    vacant), never the agent-side `other_seats` this also heals. Deliberately no
    liveness guard here: every caller (attach_session's own live-sitter refusal,
    claim_name's own live-sitter refusal, rehold_seat's own live-different-lineage
    refusal) already gathers its own, situation-specific evidence and refuses before
    ever reaching this write, exactly per this docstring's opening line. A second,
    generic guard here would not add safety; it would silently disagree with whichever
    caller-specific rule already decided this was safe (the exact
    measurements-disagreeing shape worth watching for), so this stays the bare, trusted
    write it always was."""
    now = datetime.now(UTC)
    src = source or agent_id
    seat_oid = await actions.create_or_find_object("Seat", seat_id, src)
    agent_oid = await actions.create_or_find_object("Agent", agent_id, src)
    prior = [(r["from_id"], r["canonical"]) for r in await actions.pool.fetch(
        "SELECT DISTINCT l.from_id, f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' AND f.canonical <> $2 "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", seat_oid, agent_id)]
    for old_oid, _old_canon in prior:
        await actions.invalidate_link(old_oid, seat_oid, "holds", src, now)
    other_seats = [r["to_id"] for r in await actions.pool.fetch(
        "SELECT DISTINCT l.to_id FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='holds' AND t.canonical <> $2 "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_oid, seat_id)]
    for other in other_seats:
        await actions.invalidate_link(agent_oid, other, "holds", src, now)
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='holds' "
        "AND (valid_until IS NULL OR valid_until > now()) LIMIT 1", agent_oid, seat_oid)
    if not exists:
        await actions.create_link(agent_oid, seat_oid, "holds", src, now, _CONF,
                                  evidence_class=_EC)
    return {"seat_id": seat_id, "old_holder": prior[0][1] if prior else None,
            "new_holder": agent_id}


async def rehold_seat(
    actions: Actions, *, seat_id: str, agent_id: str, because: str, actor: str,
    override_live: bool = False, dry_run: bool = False,
) -> dict[str, Any]:
    """The third-party re-hold entry point: the specimen that forced this was a compaction
    successor losing its own seat's binding to a wrongly-grafted sibling generation,
    with no sanctioned MCP verb able to put it back (`bind_holder` is a raw internal
    primitive, never exposed, and `reconcile_identity`'s third-party path only heals
    house/project property contradictions, never the `holds` link itself). This is that
    entry point: refuses when the seat's current holder is live (`seat_occupancy`, the same
    authority every other occupancy read in this codebase shares) and from a different
    lineage than `agent_id`, the exact shape a careless rehold could silently steal a
    seat out from under a genuinely different, still-working mind, unless
    `override_live=True` names that as a deliberate act (renamed from the bare
    `override` this shipped with, so the same word is used across every hold-move
    entry point's receipt/guard family; the MCP surface already spoke it this way, only the
    internal parameter lagged). `because` is required, same law as every other
    third-party correction in this codebase (a correction with no stated reason is the
    silent overwrite this rules against, not a fix).

    The retraction itself is `bind_holder`'s own already-sanctioned mechanism:
    invalidate_link, never deleted, history walkable, both sides symmetric (a stray hold
    the new holder carried elsewhere heals too). This adds only the guard, the required
    reason (kept on the seat as `rehold_because`), and a receipt naming both sides of the
    change, never a second implementation of the bind itself.

    `dry_run` (default False, since every existing caller already calls this expecting a
    real write, so flipping the default would silently no-op them; this only adds the
    flag, never changes what an omitted one does): a follow-up to a previously found bug
    where a caller passed `dry_run=True` meaning a preview and got the real write
    instead, because neither this function's own signature nor the MCP schema in front
    of it ever declared the parameter, so it silently vanished before reaching here.
    `dry_run=True` runs every guard above exactly as a real call would (a preview that
    could not actually be performed is a lie, not a preview) and returns before the
    write, honoured or refused, never silently dropped."""
    because = (because or "").strip()
    if not because:
        return {"error": "a rehold with no stated reason is exactly the silent overwrite "
                         "this system rules against, refusing"}
    seat_row = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", seat_id)
    if seat_row is None:
        return {"error": f"no such active seat: {seat_id!r}"}
    agent_row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
    if agent_row is None:
        return {"error": f"no such agent: {agent_id!r}"}
    # NEVER REHOLD ONTO A BORROWED JOB_DIR (a live specimen involving jenny/dustin):
    # never rehold onto a borrowed job_dir: the exact route that kept re-corrupting a
    # seat's binding after an equivalent guard shipped for _bind_before_spawn. A caller
    # (human or script) explicitly reholding a seat onto whatever the newest live
    # session in a project happens to be, not realizing that session's own
    # job-id-derived agent_id is a fresh, unrelated identity, not the seat's real
    # lineage. `agent_id` is real (agent_row above), but if it is actually a different,
    # real agent's own live job_dir slug, this is never a legitimate correction target:
    # a real correction names the seat's own established lineage, never a borrowed id.
    from src.orchestrator.mounts import borrowed_job_dir_owner

    borrowed_from = await borrowed_job_dir_owner(actions.pool, agent_id)
    if borrowed_from is not None:
        return {"error": f"{agent_id!r} is not a real identity, it is {borrowed_from}'s "
                         "own live job_dir slug, borrowed. A rehold must name the seat's "
                         "actual lineage (its own succeeded_from chain), never a fresh "
                         "session's job-id-derived id."}

    from src.orchestrator.agents import _generation

    occ = await seat_occupancy(actions.pool, seat_id)
    old_holder = occ["holder"]
    if (old_holder and occ["live"] and not override_live
            and _generation(old_holder)[0] != _generation(agent_id)[0]):
        return {
            "error": f"{seat_id} has a LIVE holder ({old_holder}) from a different "
                     f"lineage than {agent_id!r}, refusing without override_live=True",
            "old_holder": old_holder, "live": True,
        }
    if dry_run:
        return {"dry_run": True, "seat_id": seat_id, "old_holder": old_holder,
                "new_holder": agent_id, "because": because,
                "detail": "PREVIEW ONLY: every guard above passed and nothing was "
                          "written; call again with dry_run=False to perform this rebind"}
    now = datetime.now(UTC)
    await bind_holder(actions, seat_id=seat_id, agent_id=agent_id, source=actor)
    await actions.assert_property(seat_row["id"], "rehold_because", because, actor, now,
                                  _CONF, evidence_class=_EC)
    return {"seat_id": seat_id, "old_holder": old_holder, "new_holder": agent_id,
            "because": because}


async def backfill_unbound_seats(
    actions: Actions, *, dry_run: bool = True, only_seats: set[str] | None = None,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """The orphan-seat backfill: the batch cure for a real seat-binding gap.
    mint_heir's automatic succession only ever moves an existing `holds` link
    (follow_binding, lineage-wide), it never creates one from nothing. A seat whose
    original claim predates the Seat-object binding has therefore sat unbound through
    every generation since, however many times it has changed hands; its current
    holder calling claim_name again would fix it in one act, but nothing prompts that
    call. This finds every such seat and (dry-run by default) proposes binding it to
    whoever the assertion world already calls its live holder, the exact fallback
    resolve_seat uses for an un-seated lineage, asked in bulk rather than one name at a
    time.

    `only_seats` scopes both the plan and the write to exactly those seat ids (rolled
    out to one seat first, fleet-wide only after that lands clean); every other unbound
    seat is still counted in `total_unbound` so the caller can see what a scoped run
    deliberately left untouched, but never appears in `plan` and is never written.

    Dry-run reports the plan and writes nothing: a graph mutation is never hand-run
    without surfacing it first. Idempotent either way: bind_holder no-ops on an
    already-active link, so a repeat run, or a seat someone fixed by hand in between,
    changes nothing on its second pass. A seat with no resolvable live holder (no
    handle asserted, or the assertion world itself has no living candidate) is
    reported, never guessed."""
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
                        "note": "no handle asserted on this seat, nothing to resolve by"})
            continue
        resolved = await resolve_seat(
            actions, handle, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
        holder = resolved.get("agent")
        item: dict[str, Any] = {"seat_id": seat_id, "handle": handle, "house": house,
                                "holder": holder, "live": resolved.get("live", False)}
        if not holder:
            item["note"] = "no resolvable holder in the assertion world, skipped"
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
    """Does this mind actively hold this seat? The read side of seat-addressed mail:
    a message to `seat:<id>` is deliverable to whoever this returns True for."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical=$1 AND t.canonical=$2 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_id, seat_id))


async def seat_receipt(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any] | None:
    """The DM-receipt facts for a seat address: its display handle/house and the mind
    currently holding it (None while vacant: the mail waits; a seat address is never a
    grave, its next holder reads it). None when no such living Seat exists."""
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
    """The Seat-object world's answer to "who is <name>?": the unique living Seat
    carrying this handle, and its current holder via the active holds link. None when the
    seat world has no authoritative answer: no such seat, an ambiguous handle (two houses,
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
        # A visitor never resolves as a holder (the attach guard makes this link shape
        # impossible going forward; this is defense in depth for healed history).
        "AND NOT EXISTS (SELECT 1 FROM links sl WHERE sl.from_id=f.id "
        "  AND sl.type='spawned_by') "
        "ORDER BY l.first_seen DESC LIMIT 1", seats[0])
    if holder is None:
        return None
    return {"seat_id": seats[0], "holder": str(holder)}


async def seat_holder_ineligible(pool: asyncpg.Pool, name: str) -> str | None:
    """The missing distinction `binding_of_handle` cannot make on its own.
    `binding_of_handle` collapses four distinct failure shapes into one bare `None`: no
    such seat, an ambiguous handle, a genuinely vacant seat (no active `holds` edge at
    all), and, the case that matters here, a unique seat with an active holder who is
    marked retired/false_mint (or a spawn wearing the handle). `resolve_seat`
    (agents.py; this function never touches it) treats that bare None as "try the
    un-seated-lineage fallback instead" for all four shapes alike, and the fallback's
    own assertion-based search finds whatever other Agent object still carries a stale
    `handle` assertion for this name: a dead generation, addressed with the confidence
    of a real resolution (a message addressed by name was once delivered to a dead
    predecessor generation while a later generation was the living lineage).

    Returns None for the other three shapes (no seat / ambiguous / genuinely vacant):
    the fallback is the correct answer for an un-seated or truly-empty lineage, and this
    function must never block it. Returns a reason string, naming the seat and its
    ineligible holder(s), only for the fourth shape: a seat exists, uniquely, does have
    at least one active holder, and none of them are eligible. A caller (send_message)
    that sees this string must refuse before ever calling resolve_seat: the refusal
    belongs in the resolution, not a post-hoc check on the receipt (both `dm_to` and
    `lineage_head` agree on the same wrong answer once the fallback has already run, so
    no receipt-side check can catch this after the fact).

    Any eligible holder, not just the newest: a correction caught in review, before an
    earlier build of this deployed. `binding_of_handle` filters out marked/visitor
    holders in its own WHERE clause, then takes the newest of what remains, so a seat
    with an older eligible holder still resolves correctly even when a newer active
    holds edge belongs to a marked one. The first build of this function asked the
    wrong question ("is the newest holder eligible") instead of the one its own
    docstring already promised ("does any eligible holder exist"), and those two
    disagree exactly whenever a Seat carries more than one active `holds` edge with the
    newest marked and an older one still eligible. Not hypothetical: one real seat
    carried exactly this shape, a zero-turn phantom mint's own ordinary seam (generation
    N+1 takes the newest holds edge, gets marked false_mint seconds later, generation N
    still holds and is still eligible) reproduces it on demand. A fleet-wide single
    point of failure (send_message) must never refuse-to-serve on a check that can
    false-positive, so this now checks the filtered (eligible) set first, mirroring
    binding_of_handle exactly, and only names a refusal when that set is empty while
    active holders exist at all."""
    seats = [r["canonical"] for r in await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1)", name)]
    if len(seats) != 1:
        return None  # No seat, or an ambiguous handle: not this function's shape to name.
    rows = await pool.fetch(
        "SELECT f.canonical, "
        " EXISTS(SELECT 1 FROM current_assertions r WHERE r.object_id=f.id "
        "   AND r.name IN ('retired','false_mint') AND r.value #>> '{}' = 'true') AS marked, "
        " EXISTS(SELECT 1 FROM links sl WHERE sl.from_id=f.id "
        "   AND sl.type='spawned_by') AS visitor "
        "FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE t.canonical=$1 AND l.type='holds' AND f.type='Agent' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC", seats[0])
    if not rows:
        return None  # A genuinely vacant seat: the un-seated fallback is the right answer.
    ineligible = [r for r in rows if r["marked"] or r["visitor"]]
    if len(ineligible) < len(rows):
        return None  # At least one eligible holder: binding_of_handle resolves fine.
    names = ", ".join(
        f"{r['canonical']} ({'marked retired/false_mint' if r['marked'] else 'a visitor spawn'})"
        for r in rows)
    why = f"every active holder is ineligible ({names})"
    return (f"{seats[0]} is the unique living seat for {name!r}, but {why}, "
            "no eligible holder exists")


async def pause_seat_or_agent(
    actions: Actions, *, who: str, paused: bool, reason: str = "", actor: str,
) -> dict[str, Any]:
    """The pause write, extracted: used to be inlined in the MCP dispatcher's
    `seat(action='pause')` branch, unreachable for a console/CLI command to wire without
    duplicating the resolution order. Same resolution as always: `seat:<id>` confirmed
    living, `agent:<id>` resolved through its lineage's own living head to whichever seat
    it holds (falling back to the head itself when unseated), or a bare name resolved
    the same way a DM address is (refusing loudly on `seat_holder_ineligible`'s own
    shape rather than silently falling through to a dead generation). Stamps
    `paused`/`paused_reason` on whichever object (Seat or Agent) the resolution landed
    on, counts this address's own queued-but-unread DMs, and returns the exact receipt
    shape the MCP tool always has.

    `who` is the caller's own resolved target string (already defaulted to the caller's
    own agent id upstream when no explicit target was given): this function never
    re-derives that default, it only resolves whatever string it's handed."""
    pool = actions.pool
    from src.orchestrator.agents import resolve_seat
    from src.orchestrator.folds import canonical_agent, living_head

    if who.startswith("seat:"):
        if await seat_receipt(pool, who) is None:
            return {"error": f"no such living seat: '{who}', check fleet()"}
        stamp_on = who
    elif who.startswith("agent:"):
        head = await living_head(pool, await canonical_agent(pool, who))
        bound = await held_seat(pool, head)
        stamp_on = (bound or {}).get("seat_id") or head
    else:  # a plain name: resolve like a DM address does
        ineligible = await seat_holder_ineligible(pool, who)
        if ineligible is not None:
            return {"error": f"cannot pause '{who}': {ineligible}. Address the seat "
                             "directly (target='seat:<id>') once a new holder claims "
                             "it, or pause the seat id itself if you mean to gate the "
                             "chair."}
        resolved = await resolve_seat(actions, who)
        if resolved["agent"] is None:
            return {"error": f"no seat or agent named '{who}', check fleet()"}
        stamp_on = resolved.get("seat_id") or resolved["agent"]
    obj_type = "Seat" if stamp_on.startswith("seat:") else "Agent"
    oid = await actions.create_or_find_object(obj_type, stamp_on, actor)
    now = datetime.now(UTC)
    await actions.assert_property(oid, "paused", paused, actor, now, 0.9,
                                  evidence_class="self_declared")
    if reason:
        await actions.assert_property(oid, "paused_reason", reason[:500], actor, now,
                                      0.9, evidence_class="self_declared")
    queued = 0
    if stamp_on.startswith("agent:") or stamp_on.startswith("seat:"):
        queued = await pool.fetchval(
            "SELECT count(*) FROM fleet_messages m WHERE m.to_agent=$1 AND "
            "m.read_at IS NULL AND NOT EXISTS (SELECT 1 FROM message_recipients r "
            "WHERE r.message_id=m.id AND r.read_at IS NOT NULL)", stamp_on) or 0
    return {"paused" if paused else "released": stamp_on, "by": actor,
           **({"reason": reason} if reason else {}),
           **({"queued_dms": queued} if queued else {}),
           "note": ("the DM push lane now queues this seat's mail, release with "
                    "seat(action='pause', paused=False, target=...)" if paused else
                    "the queue drains on the next dispatch (a fresh send, or the "
                    "worker sweep within the minute)")}


async def _seat_display(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any]:
    """Handle and house for a seat address. `house` is derived, never read from this
    seat's own stored `house` property: reading the raw stamp here was the same bypass
    seat_bearings shipped, just for mail receipts (seat_receipt) instead of orient()."""
    row = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS handle "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Seat' AND o.status='active'", seat_id)
    if row is None:
        return {}
    return {"handle": row["handle"], "house": await derive_house(pool, seat_id)}


async def attach_session(
    actions: Actions, *, seat_id: str, token: str, job_dir: str, agent_id: str,
    live_secs: int = _LIVE_SECS,
) -> dict[str, Any]:
    """Verify the token, then bind the session to its Seat. Refusals are loud (an error
    dict the whisper prints) and write nothing; only a verified fresh use, or the same
    presenter resuming, touches the tables."""
    pool = actions.pool
    row = await pool.fetchrow(
        "SELECT seat_id, used_by, used_at FROM seat_tokens WHERE token=$1", token)
    if row is None:
        return {"error": "ATTACH REFUSED, unknown attach token: nothing was bound. "
                         "The token in this environment matches no mint on record."}
    if row["seat_id"] != seat_id:
        return {"error": f"ATTACH REFUSED: token/seat mismatch: this token was minted for "
                         f"{row['seat_id']}, not {seat_id}. A stale or foreign environment; "
                         "nothing was bound."}
    display = await _seat_display(pool, seat_id)
    if not display:
        return {"error": f"ATTACH REFUSED: {seat_id} is not a living Seat in the graph; "
                         "nothing was bound."}
    if row["used_at"] is not None and row["used_by"] != job_dir:
        # Refuses both the env-inheritance leak and the collision class in one breath:
        # the first presenter owns the token, forever.
        return {"error": f"ATTACH REFUSED: this token was already used by another session "
                         f"({row['used_by']}). A one-time token binds to its first "
                         "presenter; a second presentation is an inherited environment or a "
                         "collision, never a resume. Nothing was bound."}
    # A visitor never holds a seat: a sub-agent works in its parent's name, and binding
    # one would seat a sidechain that dies with its task and never resumes.
    spawner = await pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects p ON p.id=l.to_id "
        "WHERE f.canonical=$1 AND l.type='spawned_by' LIMIT 1", agent_id)
    if spawner:
        return {"error": f"ATTACH REFUSED: {agent_id} is a VISITOR (spawned_by {spawner}); "
                         "a sub-agent never holds a seat. Nothing was bound."}
    fresh_use = row["used_at"] is None
    if fresh_use:
        holder = await pool.fetchrow(
            "SELECT job_dir, agent_id FROM agent_mounts WHERE seat_id=$1 AND job_dir<>$2 "
            "AND last_seen > now() - make_interval(secs => $3) "
            "ORDER BY last_seen DESC LIMIT 1", seat_id, job_dir, live_secs)
        if holder is not None:
            return {"error": f"ATTACH REFUSED: {seat_id} ({display.get('handle')}) is held "
                             f"LIVE by {holder['agent_id']}. Two minds in one seat is the "
                             "collision class the ceremony exists to kill; nothing was bound."}
        claimed = await pool.execute(
            "UPDATE seat_tokens SET used_by=$2, used_at=now() "
            "WHERE token=$1 AND used_at IS NULL", token, job_dir)
        if claimed.rsplit(" ", 1)[-1] == "0":
            # raced by a concurrent presenter: re-read, only the same job_dir may proceed
            again = await pool.fetchrow(
                "SELECT used_by FROM seat_tokens WHERE token=$1", token)
            if again is None or again["used_by"] != job_dir:
                return {"error": "ATTACH REFUSED: token claimed concurrently by another "
                                 "session; nothing was bound."}
    bound = await pool.execute(
        "UPDATE agent_mounts SET seat_id=$2 WHERE job_dir=$1", job_dir, seat_id)
    if bound.rsplit(" ", 1)[-1] == "0":
        return {"error": "ATTACH REFUSED: no durable mount row for this session yet; the "
                         "binding needs the mount to exist first (automount runs it in "
                         "order). Nothing was bound."}
    # the holds link: the previous holder's heals (valid_until), never deleted
    await bind_holder(actions, seat_id=seat_id, agent_id=agent_id)
    return {"attached": seat_id, "handle": display.get("handle"),
            "house": display.get("house"), "agent": agent_id,
            "resumed": not fresh_use}


async def follow_binding(
    actions: Actions, *, ancestor_oid: uuid.UUID, heir: str, heir_oid: uuid.UUID,
    now: datetime,
) -> list[dict[str, Any]]:
    """The binding follows the lineage head (mint_heir's hook): every Seat the lineage
    actively holds re-links to the heir. The old link heals by valid_until, the seat's
    holder history stays walkable, and seat-addressed anything keeps reaching whoever the
    mind is now. No seat, no-op.

    Returns one `{"seat_id", "old_holder", "new_holder"}` per seat actually moved: a live
    sibling's seat, skipped by the guard below, simply never appears in the list, so a
    caller can tell "nothing to move" from "moved nothing because a live sibling held it"
    by checking this against the lineage's own known holds. No separate `override_live`
    here: this fires automatically inside mint_heir's own succession hook, with no
    operator present to supply one, so the guard stays unconditional by design, unlike
    rehold_seat's deliberate third-party entry point.

    Lineage-wide, not ancestor-only: churn can leave the active holds link on a folded
    sibling rather than the direct ancestor, so a mint from the living head could find
    nothing to move and leave mail to the seat undelivered. Any active holds link
    anywhere in the heir's lineage (or on the explicit ancestor, which a cross-base
    succession may place outside it) re-links to the heir, except a sibling generation's
    hold that is itself genuinely live right now (a fresh mint must never steal an
    unrelated, still-working sibling's seat out from under it). Liveness here is checked
    exact-generation, never through `mounts.agent_liveness`'s own lineage-wide widening:
    every row already shares the heir's base by construction, so that widened check would
    read every sibling "live" the instant the heir's own fresh mount row lands, defeating
    the very guard this adds. The ancestor's own hold always moves, live or not: an
    ancestor never contests its own heir."""
    from src.orchestrator.agents import _generation

    base = _generation(heir)[0]
    seats = await actions.pool.fetch(
        "SELECT l.from_id, l.to_id, hf.canonical AS holder, t.canonical AS seat_id "
        "FROM links l JOIN objects hf ON hf.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE l.type='holds' AND l.from_id <> $3 "
        "AND (l.from_id=$1 OR hf.canonical=$2 OR hf.canonical LIKE $2 || '-%') "
        "AND (l.valid_until IS NULL OR l.valid_until > now())",
        ancestor_oid, base, heir_oid)
    moved: list[dict[str, Any]] = []
    for r in seats:
        if r["from_id"] != ancestor_oid and await _exact_holder_live(actions.pool, r["holder"]):
            continue
        await actions.invalidate_link(r["from_id"], r["to_id"], "holds", heir, now)
        await actions.create_link(heir_oid, r["to_id"], "holds", heir, now, _CONF,
                                  evidence_class=_EC)
        moved.append({"seat_id": r["seat_id"], "old_holder": r["holder"], "new_holder": heir})
    return moved


async def _exact_holder_live(pool: asyncpg.Pool, canonical: str) -> bool:
    """Exact-generation liveness for one specific holder id: `follow_binding`'s own guard,
    deliberately not `mounts.agent_liveness` (which widens across the whole lineage base).
    Called from inside a sweep where every candidate already shares that same base, the
    widened check would report every sibling live the moment the heir itself has a fresh
    mount row, which is exactly the false positive this exists to avoid. Delegates to the
    shared primitive (`mounts.agent_liveness_exact`, promoted from this function's own
    original inline body when `correct_succession` needed the identical exact-match check)
    rather than keeping a second copy to drift from."""
    from src.orchestrator.mounts import agent_liveness_exact

    return bool((await agent_liveness_exact(pool, canonical))["live"])


# ═══ SEAT LIFECYCLE: self-organizing seats. A head corrects its own anchor, a
# duplicate seat folds deliberately, a genuinely dead role retires. None of this is
# fenced to the operator's hand: each is an identity act within its own caller's
# authority, the same law claim_name already runs on.


async def correct_house(actions: Actions, agent_id: str, new_house: str, *, source: str,
                        ) -> dict[str, Any]:
    """A head corrects its own stored house: the one legitimate write left after
    derive_house. A head's anchor is a deliberate identity declaration, exactly like
    claim_name, so this is self-scoped and not operator-fenced: an identity act within
    its own authority.

    Refuses loudly on: an empty house; a caller holding no seat; a caller whose seat is
    not a head (an active managed_by edge out means this seat derives its house through
    its manager now, so stamping its own house property would be inert data nobody
    reads, the exact "legacy write stays inert" shape derive_house already documents).

    Prior art is surfaced, never refused: the receipt's own `prior_art`/`prior_art_flag`
    keys, when present, name a standing Decision that may already cover this seat's
    house, the same search()-based guard record_decision runs on itself, generalized
    here. This cannot distinguish a deliberate correction from an uninformed overwrite;
    it only ensures the write does not land silently unread.

    A fourth edge case is named in the receipt: `was == new_house` means this call's own
    declaration was already true before it ran. The write above still lands (a fresh
    assertion, harmless, matching the append-only discipline), but nothing was actually
    corrected, and a verb whose name promises correction owes the caller that word
    instead of a receipt indistinguishable from a real one. `already_correct` says so
    plainly. Separately, and this is the part `was` alone could never say: other sources
    may still carry a contradicting `house` value for this seat, never invalidated by
    this or any call. `still_contradicted` lists them (source, value, observed_at),
    read-only, so a caller who declared the truth weeks ago and still shows up in a
    "contradicted" triage bucket can see exactly why: declaring is not the missing step,
    invalidating is, and this verb still does not do it (that is the read side, not this
    write)."""
    new_house = (new_house or "").strip()
    if not new_house:
        return {"error": "a house needs a name"}
    bound = await held_seat(actions.pool, agent_id)
    if bound is None:
        return {"error": f"{agent_id} holds no seat, house-correct is a seat's own act, "
                         "never done on another's behalf"}
    seat_id = bound["seat_id"]
    manager = await manager_of_seat(actions.pool, seat_id)
    if manager is not None:
        return {"error": f"{seat_id} is managed_by {manager}, not a head. A non-head "
                         f"derives its house through the chain (currently {bound['house']!r}); "
                         "only a head's own stamp is ever read, so only a head may correct "
                         "one. Nothing to do here."}
    was = bound.get("house")
    seat_obj = await actions.create_or_find_object("Seat", seat_id, source)
    await actions.assert_property(seat_obj, "house", new_house, source, datetime.now(UTC),
                                  _CONF, evidence_class=_EC)
    # Self-heal at write time: a deliberate declaration is exactly the moment a
    # cross-source contradiction either gets created or gets a chance to heal, so never
    # leave the new value sitting beside a stale one for a human to notice later.
    from src.orchestrator.identity_heal import heal_contradicting_property
    await heal_contradicting_property(actions, object_id=seat_obj, name="house", actor=source)
    from src.orchestrator.capture import property_prior_art

    prior_art_bits = await property_prior_art(
        actions.pool, subject_canonical=seat_id, field="house", new_value=new_house,
        actor=source)
    contradicting = await actions.pool.fetch(
        "SELECT a.value #>> '{}' AS val, a.source_id, a.observed_at "
        "FROM current_assertions a WHERE a.object_id=$1 AND a.name='house' "
        "AND a.value #>> '{}' != $2 ORDER BY a.observed_at DESC", seat_obj, new_house)
    still_contradicted = [
        {"value": r["val"], "source": r["source_id"], "observed_at": r["observed_at"].isoformat()}
        for r in contradicting]
    return {"seat_id": seat_id, "house": new_house, "was": was,
            "already_correct": was == new_house, "still_contradicted": still_contradicted,
            **prior_art_bits}


async def resync_seat_house_third_party(
    actions: Actions, seat_id: str, new_house: str | None, *, source: str, reason: str,
) -> dict[str, Any]:
    """The third-party sibling of `correct_house`: `correct_house` is deliberately
    self-scoped (a head correcting its own identity declaration), but a stale Seat.house
    left behind by a `rename_project` that never propagates to the seat itself (the
    schema was silent on this exact property: `Seat.house` written once at `ensure_seat`
    mint time, never resynced by anything since) is not the seat's own act to make. It is
    an individually-diagnosed, individually-authorized correction landed by someone else,
    on the same explicit-reason audit-trail discipline as `offices.correct_pin_value`.
    Refuses on an empty reason for the exact same cause that function refuses one: a
    correction with no stated reason is the silent overwrite this house rules against,
    not a fix. Does not check headship or caller identity: this is explicitly a
    third-party act, unlike `correct_house`, and callers are responsible for the
    authorization this docstring cannot enforce.

    `new_house=None` unsets the property (repairing the fabricated-house pattern where
    house was stamped `=handle` at mint) to genuinely unset, never a placeholder. The
    `assertions.value` column is NOT NULL at the schema level: asyncpg maps a Python
    `None` parameter to SQL NULL universally, before the jsonb codec ever runs, so a
    null-valued assertion cannot be written at all. Stored as the empty string instead
    (`""`), the same sentinel `derive_house`/`_own_house_stamp` already treat as "no
    house" (a genuinely empty derived house is treated like "no seat yet"): every
    existing reader already does a truthy check, not an `is None` check, so this is not
    a new state to teach anything, only a new route to reach the state through. The
    receipt always reports `None`, never `""`, for a clean external contract.

    `still_contradicted` (the same fourth-edge-case fix as `correct_house`): names any
    other source's lingering `house` value neither branch here touches. Writing or
    confirming the correct value is not the same act as invalidating a stale one."""
    if new_house is not None:
        new_house = new_house.strip() or None
    stored = new_house or ""  # the NOT NULL-safe encoding of "unset", see the note above
    if not reason.strip():
        return {"error": "a correction with no reason is exactly the silent overwrite "
                         "this system rules against, refusing"}
    # The existence check: this call used to reach straight for create_or_find_object
    # below with no check the seat existed at all, so a nonexistent-seat "safe"
    # invocation would silently mint a stray Seat rather than refuse, unlike
    # reconcile_seat_identity_third_party, this function's own precedent-named sibling,
    # which does refuse (identity_heal.py's own `SELECT ... WHERE canonical=$1 AND
    # type='Seat' AND status='active'` check). Same query, same refusal shape, mirrored
    # here rather than invented fresh.
    seat_row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if seat_row is None:
        return {"error": f"no active seat matches {seat_id!r}"}
    facts = await seat_facts(actions.pool, seat_id)
    was = facts.get("house") or None  # normalize a stored "" back to None for the receipt
    seat_obj = await actions.create_or_find_object("Seat", seat_id, source)
    written = was != new_house
    if written:
        await actions.assert_property(seat_obj, "house", stored, source, datetime.now(UTC),
                                      _CONF, evidence_class=_EC)
        # Self-heal at write time, same reasoning as correct_house.
        from src.orchestrator.identity_heal import heal_contradicting_property
        await heal_contradicting_property(actions, object_id=seat_obj, name="house",
                                          actor=source)
    # read AFTER any write, so `still_contradicted` reflects the state this call actually
    # leaves behind (a same-source prior value is superseded by the write above and drops
    # out here; a different-source value survives untouched either way).
    contradicting = await actions.pool.fetch(
        "SELECT a.value #>> '{}' AS val, a.source_id, a.observed_at "
        "FROM current_assertions a WHERE a.object_id=$1 AND a.name='house' "
        "AND a.value #>> '{}' != $2 ORDER BY a.observed_at DESC",
        seat_obj, stored)
    still_contradicted = [
        {"value": (r["val"] or None), "source": r["source_id"],
         "observed_at": r["observed_at"].isoformat()}
        for r in contradicting]
    if not written:
        return {"written": False, "seat_id": seat_id, "house": was,
                "still_contradicted": still_contradicted}
    return {"written": True, "seat_id": seat_id, "house": new_house, "was": was,
            "reason": reason, "still_contradicted": still_contradicted}


async def resync_seat_project(
    actions: Actions, seat_id: str, *, source: str, reason: str,
) -> dict[str, Any]:
    """One repair entry point, not two: collapses `correct_house` (self-scoped, head only) and
    `resync_seat_house_third_party` (third-party, took an arbitrary declared value)
    into a single entry point that re-derives a seat's own stamped house/project property from
    its own charter. A seat's project is never a second, independently-declared value,
    so there is nothing left here to self-scope or to hand a caller-chosen value: works
    for any seat, on a stated reason, same third-party discipline
    `resync_seat_house_third_party` always held.

    The mechanism below this property is untouched (derive_house): this only ever
    writes the same `house` property derive_house already reads for a head seat's own
    stamp. The house-anchor/ghost-clause boundary-crossing logic that protects against a
    repeat of a past cross-seat annexation never changes shape; it just keeps reading
    whatever this entry point writes, exactly as it read whatever
    correct_house/resync_seat_house_third_party used to write.

    Refuses on: an empty reason (a correction with no stated reason is exactly the
    silent overwrite this house rules against); an unknown/inactive seat; a charter
    governing zero projects (nothing to derive from: never invents one, the same "never
    fabricate, leave genuinely unset" law this property has always held); a charter
    governing more than one (ambiguous: no single project to derive, names them all,
    never guesses)."""
    if not reason.strip():
        return {"error": "a correction with no reason is exactly the silent overwrite "
                         "this system rules against, refusing"}
    seat_row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat' AND status='active'",
        seat_id)
    if seat_row is None:
        return {"error": f"no active seat matches {seat_id!r}"}
    from src.orchestrator.charter import charter_of, project_current_name

    governed = await charter_of(actions.pool, seat_id)
    if not governed:
        return {"error": f"{seat_id} has no charter, nothing to derive a project from"}
    if len(governed) > 1:
        return {"error": f"{seat_id}'s charter governs {len(governed)} projects "
                         f"({', '.join(governed)}), ambiguous, no single project to "
                         "derive"}
    # The current name, never the canonical: governed[0] is charter_of's own
    # frozen-at-mint canonical label, and a project renamed since would stamp the
    # seat's own house with the stale label forever.
    new_project = await project_current_name(actions.pool, governed[0])
    facts = await seat_facts(actions.pool, seat_id)
    was = facts.get("house") or None
    seat_obj = await actions.create_or_find_object("Seat", seat_id, source)
    await actions.assert_singular_property(
        seat_obj, "house", new_project, source, datetime.now(UTC), _CONF,
        because=f"{reason} (resync_seat_project: re-derived from its own charter, "
                "cross-source collapse, ruling 1335332e)",
        evidence_class=_EC)
    from src.orchestrator.identity_heal import heal_contradicting_property
    await heal_contradicting_property(actions, object_id=seat_obj, name="house",
                                      actor=source)
    return {"seat_id": seat_id, "project": new_project, "was": was,
            "already_correct": was == new_project, "reason": reason}


async def _move_seat_estate(
    actions: Actions, dupe_oid: uuid.UUID, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """The seat estate-move itself, factored out of `fold_seat` so `reconcile_seat_fold`
    can run the exact same repair on an already-merged pair rather than a second
    implementation that could drift from what a normal fold already does. Every item here
    is individually idempotent: a holder or managed_by edge already re-pointed, or mail
    already moved, no longer matches its own WHERE clause, so running this twice, or
    running it after `fold_seat`'s own inline call already did the same work, changes
    nothing on the second pass."""
    now = datetime.now(UTC)
    into_obj = await actions.create_or_find_object("Seat", into, actor)
    holders = await actions.pool.fetch(
        "SELECT f.id AS fid, f.canonical AS holder FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen ASC", dupe_oid)
    for row in holders:
        await actions.invalidate_link(row["fid"], dupe_oid, "holds", actor, now)
        await bind_holder(actions, seat_id=into, agent_id=row["holder"], source=actor)
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
    mail_tag = await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        into, dupe)
    mail_moved = int(mail_tag.rsplit(" ", 1)[-1])
    return {"holders_moved": [r["holder"] for r in holders],
            "managed_by_moved": len(managing) + len(managed), "mail_moved": mail_moved}


async def fold_seat(
    actions: Actions, *, dupe: str, into: str, evidence: str, actor: str,
) -> dict[str, Any]:
    """Fold seat `dupe` into seat `into`: the deliberate, evidence-gated cure for two
    Seat objects that should have been one (the concrete case this exists for: a
    resolution-order bug in claim_name minted a second seat while the real one, managed
    by another head, sat vacant).

    Unlike fold_agent (folds.py, which refuses outright when `dupe` holds a seat,
    because a seat transfer is a deliberate act there, never a fold's side effect):
    fold_seat's whole job is moving active holders. Every live `holds` link on `dupe`
    re-points to `into`. If `dupe` had more than one concurrent holder (the duplicate's
    own anomaly, the thing this verb exists to close, not preserve), they converge to
    one: bind_holder's own succession law means whichever is re-pointed last survives as
    `into`'s active holder, so this processes oldest-first, meaning the newest holder
    wins, matching every recency-wins convention elsewhere in this codebase. All are
    still named in `holders_moved`. `managed_by` edges move too, in either direction, so
    a folded seat's own org-chart position (who it managed, who managed it) survives the
    merge. Mail addressed to `dupe` follows to `into`.

    Refuses loudly, nothing written, on: empty evidence (an auto-merge wearing a
    signature); either label unknown or not a Seat; dupe==into; dupe already folded."""
    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not (evidence or "").strip():
        return {"error": "a fold without evidence is an auto-merge wearing a signature, "
                         "cite what proves these are one seat"}
    if not dupe or not into:
        return {"error": "fold_seat needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same seat, nothing to fold"}
    rows = await actions.pool.fetch(
        "SELECT id, canonical, status FROM objects WHERE canonical = ANY($1::text[]) "
        "AND type='Seat'", [dupe, into])
    by_label = {r["canonical"]: r for r in rows}
    if dupe not in by_label or into not in by_label:
        missing = [x for x in (dupe, into) if x not in by_label]
        return {"error": f"unknown seat(s): {', '.join(missing)}, a fold never invents "
                         "either side"}
    if by_label[dupe]["status"] == "merged":
        return {"error": f"{dupe} is already folded, nothing to do"}
    if by_label[into]["status"] == "merged":
        return {"error": f"{into} is itself folded, fold into the living seat instead"}
    dupe_oid, into_oid = by_label[dupe]["id"], by_label[into]["id"]
    # The estate, seat-shaped: active holders move first, the point of this verb, where
    # fold_agent refuses instead (bind_holder's own succession law converges a
    # duplicate's multiple concurrent holders to the newest one, matching every
    # recency-wins convention elsewhere in this codebase). Mail and managed_by follow
    # too, before the kernel merge (mirroring fold_project/fold_agent's shared pattern):
    # a crash between here and the merge below leaves dupe.status=='active', so a retry
    # continues rather than hitting the merge's own "already folded" refusal with estate
    # stranded on dupe forever.
    estate = await _move_seat_estate(actions, dupe_oid, dupe, into, actor)
    # the kernel merge: event, projection, resolve-on-read, the same primitive fold_agent
    # itself calls, type-agnostic, no Agent-only check inside it
    await actions.merge_objects(into_oid, dupe_oid, justification=evidence, actor=actor)
    return {"folded": dupe, "into": into, **estate}


async def reconcile_seat_fold(
    actions: Actions, *, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """The repair path fold_seat never had: folds are idempotent-by-refusal when they
    need to be idempotent-by-repair. Re-points any live holder/managed_by/mail estate
    item still aimed at an already-merged dupe, using the same `_move_seat_estate`
    `fold_seat` itself calls, not a second implementation that could drift from what a
    normal fold already does.

    The inverse precondition of fold_seat, on purpose, so the two verbs' refusal
    conditions never overlap: fold_seat requires status=='active' and refuses an
    already-folded dupe; reconcile requires dupe.status=='merged' and dupe's own
    `merged_into` pointing at exactly `into` (refuses to redirect a dupe merged into
    some other seat, never guesses which pair a caller means). Never re-performs the
    fold: no `merge_objects` call.

    No actor gate, matching `fold_seat`'s own current (flagged, unfixed) posture: this
    build inherits that asymmetry consistently rather than reconciling it now.

    Refuses loudly on: blank dupe/into; dupe==into; dupe not resolving to a Seat;
    dupe.status != 'merged' (fold_seat's job, not this one's); dupe's own `merged_into`
    not equal to `into`'s id; into not resolving to an active Seat."""
    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not dupe or not into:
        return {"error": "reconcile_seat_fold needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same seat, nothing to reconcile"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Seat'",
        dupe)
    if row is None:
        return {"error": f"no such seat: {dupe!r}, reconcile never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is {row['status']}, not merged. reconcile_seat_fold "
                         "only repairs an ALREADY-completed fold; use merge to fold it in "
                         "the first place"}
    into_row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", into)
    if into_row is None:
        return {"error": f"no such seat: {into!r}, reconcile never invents a label"}
    if into_row["id"] != row["merged_into"]:
        actual = await actions.pool.fetchval(
            "SELECT canonical FROM objects WHERE id=$1", row["merged_into"])
        return {"error": f"{dupe} is merged into {actual}, not {into}. "
                         "reconcile_seat_fold never redirects to a different pair"}
    if into_row["status"] != "active":
        return {"error": f"{into} is {into_row['status']}, not active"}
    estate = await _move_seat_estate(actions, row["id"], dupe, into, actor)
    return {"reconciled": dupe, "into": into, **estate}


async def unfold_seat(
    actions: Actions, *, dupe: str, because: str, actor: str, execute: bool = False,
) -> dict[str, Any]:
    """Reverse a wrongful fold_seat: the Seat sibling of `folds.unfold_agent`, built for
    parity (fold_seat shipped with no reversal at all, so a fold here was permanent and
    unrepairable). Dry run is the default (`execute=False`): returns the plan without
    writing.

    Refuses loudly (an error dict, nothing written) when: `because` is blank; `dupe` is
    unknown or not currently folded (status != 'merged'); the original fold's own
    justification cites a standing ruling and `because` does not also carry a fresh
    justification, the same discipline `unfold_agent` already holds, generalized to
    seats.

    Estate, seat-shaped: fold_seat's holders and managed_by moves are event-sourced
    (`invalidate_link` + `create_link`) and are restored automatically when and only
    when nothing has touched them since (`folds._reversible_moved_links`: a holder now
    on some other seat, or a managed_by edge since re-pointed again, is never guessed
    back). Mail was moved by a raw UPDATE (the same as `fold_agent`'s own mail leg) and
    is never reversible: always reported as `estate_unreturnable`, never restored."""
    from src.orchestrator.folds import (
        _reversible_moved_links,
        fold_justification_and_parity_check,
    )

    dupe, because = (dupe or "").strip(), (because or "").strip()
    if not because:
        return {"error": "an unfold without a because is an un-audited reversal, cite "
                         "the evidence/ruling that proves the fold was wrong"}
    if not dupe:
        return {"error": "unfold_seat needs a dupe label"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Seat'",
        dupe)
    if row is None:
        return {"error": f"unknown seat: {dupe}, an unfold never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is not folded (status={row['status']}), nothing to "
                         "unfold"}
    into_id = row["merged_into"]
    into_canon = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", into_id)
    ev, original_evidence, parity_error = await fold_justification_and_parity_check(
        actions.pool, row["id"], because, label=dupe)
    if parity_error is not None:
        return parity_error

    holders = await _reversible_moved_links(actions.pool, dupe_id=row["id"], into_id=into_id,
                                            link_type="holds", from_dupe=True)
    managing_out = await _reversible_moved_links(  # dupe's OWN managers (dupe managed_by X)
        actions.pool, dupe_id=row["id"], into_id=into_id, link_type="managed_by",
        from_dupe=False)
    managing_in = await _reversible_moved_links(  # dupe's subordinates (X managed_by dupe)
        actions.pool, dupe_id=row["id"], into_id=into_id, link_type="managed_by",
        from_dupe=True)
    unreturnable_mail = [dict(r) for r in await actions.pool.fetch(
        "SELECT id, from_agent, created_at, read_at, left(body,120) AS body "
        "FROM fleet_messages WHERE to_agent=$1 AND created_at <= $2 ORDER BY created_at",
        into_canon, ev["created_at"] if ev else datetime.now(UTC))]

    plan: list[dict[str, Any]] = [
        {"op": "unmerge_objects", "target": dupe, "detail": f"status merged→active, "
         f"merged_into cleared (was {into_canon})"}]
    for h in holders:
        plan.append({"op": "move_link", "target": h["label"], "detail":
                    f"holds {into_canon} → {dupe} (restoring the pre-fold holder)"})
    for m in managing_out:
        plan.append({"op": "move_link", "target": m["label"], "detail":
                    f"managed_by {into_canon} → {dupe} (dupe's own manager)"})
    for m in managing_in:
        plan.append({"op": "move_link", "target": m["label"], "detail":
                    f"managed_by {into_canon} → {dupe} (dupe's subordinate)"})

    report: dict[str, Any] = {
        "dupe": dupe, "was_merged_into": into_canon,
        "fold_actor": ev["actor"] if ev else None, "fold_justification": original_evidence,
        "plan": plan,
        "estate_unreturnable": {
            "mail": unreturnable_mail,
            "note": ("pre-fold UPDATEs overwrote to_agent in place: these predate the "
                     "fold and still sit on the living seat, but nothing proves they were "
                     "ever addressed to dupe rather than already into's own; read them "
                     "and judge by hand, never auto-moved") if unreturnable_mail else
                    "none found, no pre-fold mail sits unclaimed on the living seat",
        },
        "execute": execute,
    }
    if not execute:
        return report

    now = datetime.now(UTC)
    await actions.unmerge_objects(row["id"], because, actor)
    for h in holders:
        await actions.invalidate_link(h["fid"], into_id, "holds", actor, now)
        await actions.create_link(h["fid"], row["id"], "holds", actor, now, _CONF,
                                  evidence_class=_EC)
    for m in managing_out:
        await actions.invalidate_link(into_id, m["fid"], "managed_by", actor, now)
        await actions.create_link(row["id"], m["fid"], "managed_by", actor, now, _CONF,
                                  evidence_class=_EC)
    for m in managing_in:
        await actions.invalidate_link(m["fid"], into_id, "managed_by", actor, now)
        await actions.create_link(m["fid"], row["id"], "managed_by", actor, now, _CONF,
                                  evidence_class=_EC)
    report.update({
        "unmerged": True, "holders_restored": len(holders),
        "managed_by_restored": len(managing_out) + len(managing_in),
        "note": (f"{dupe} is active again, provenance for the folded era stays on the "
                 "record (the merge event and same_as link are witnesses, never erased). "
                 + (f"{len(holders)} holder(s) restored. " if holders else "")
                 + (f"{len(managing_out) + len(managing_in)} managed_by edge(s) restored. "
                    if (managing_out or managing_in) else "")
                 + ("Unreturnable mail is listed above for a human to judge by hand."
                    if unreturnable_mail else "")),
    })
    return report


async def retire_seat(actions: Actions, seat_id: str, *, reason: str = "", actor: str,
                      ) -> dict[str, Any]:
    """Mark a Seat permanently closed: a genuinely dead role, no successor, no merge
    target (fold_seat is for a duplicate; this is for a role that's simply over).
    Distinct from the session-level retire() (mcp_server.py), which retires a live
    agent's own turn; this retires the role itself, for every mind that ever might hold
    it.

    Refuses loudly on: unknown or already-inactive seat; an active holder (a live mind
    sitting in a seat is not this verb's business to evict: transfer or let it vacate
    first, the same discipline fold_agent already holds for agents); an active peer_of
    edge (a peered seat retiring first would leave the bond pointing at a dead seat
    forever: unpeer first, same reasoning as the holder guard).

    The status gap: this used to stamp only the `retired` property, leaving
    objects.status reading 'active' forever, invisible to anything that checks status
    rather than the property, and nothing stopped a fresh claim from re-binding to a
    seat that looked retired. Now flips both layers: the property (for anything already
    reading it) and objects.status via Actions.set_status (the real compensating event,
    same pattern as retire_project), so a retired seat is actually inert, not just
    labeled.

    The peer guard: unpeer requires both seats active to resolve them, so retiring a
    peered seat first, with nothing else stopping that, left the bond stuck active
    forever, pointing at a dead seat with no sanctioned verb able to heal it (the
    identical class of bug as a fold/bond that outlives the object it names). Refusing
    here, symmetrically with the holder check, keeps the fix on the retire side rather
    than loosening unpeer's own active-seat guard, which would weaken a real invariant
    elsewhere."""
    seat_id = (seat_id or "").strip()
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    if row["status"] != "active":
        return {"error": f"{seat_id} is already {row['status']}, nothing to retire"}
    holder = await actions.pool.fetchval(
        "SELECT f.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) LIMIT 1", row["id"])
    if holder:
        return {"error": f"{seat_id} is actively held by {holder}, retire_seat never "
                         "evicts a live mind; transfer or vacate the seat first"}
    peer = await _active_peer(actions.pool, row["id"])
    if peer is not None:
        return {"error": f"{seat_id} is peered with {peer['peer']}, retiring it would "
                         "strand that bond pointing at a dead seat forever; unpeer first"}
    await actions.assert_property(row["id"], "retired", "true", actor, datetime.now(UTC),
                                  _CONF, evidence_class=_EC)
    if (reason or "").strip():
        await actions.assert_property(row["id"], "retired_because", reason.strip(), actor,
                                      datetime.now(UTC), _CONF, evidence_class=_EC)
    await actions.set_status(row["id"], "retired", reason.strip() or "seat retired", actor)
    return {"retired": seat_id}


async def vacate_holder(
    actions: Actions, *, seat_id: str, actor: str, because: str,
) -> dict[str, Any]:
    """Release a seat's active holder(s) without binding a new one: the deliberate-hand
    complement to bind_holder (which only ever moves the link onto a new holder) and to
    retire_seat's own stale-holder refusal (which is right to refuse: retire_seat closes
    the role, and evicting a live mind is not its business). This is for the one case
    that refusal correctly can't resolve on its own: a holder whose process is confirmed
    dead without ever calling retire() on itself (a `claude stop`ped body leaves its
    `holds` link stale forever, with nothing to release it).

    This verb trusts its caller. It does no liveness check of its own: that evidence is
    trigger.py's job (vacate_dead_seat, the only sanctioned caller), which reads the real
    process roster and the transcript's own timestamped content before ever reaching
    here, exactly as retire_seat's docstring already distinguishes "the graph's word"
    from "an actual eviction." Calling this directly on a genuinely live holder is a
    caller error, not a refusal this function can catch.

    Refuses loudly on: an unknown/inactive seat, a blank `because` (the same law
    set_seat_attended already holds: a seat's occupancy changing this way belongs on the
    record), or a seat with no active holder (nothing to vacate)."""
    if not because.strip():
        return {"error": "because is required: vacating a seat's holder is a deliberate "
                         "act on the record"}
    seat_id = (seat_id or "").strip()
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    if row["status"] != "active":
        return {"error": f"{seat_id} is already {row['status']}, nothing to vacate"}
    now = datetime.now(UTC)
    holders = await actions.pool.fetch(
        "SELECT f.id AS fid, f.canonical AS holder FROM links l "
        "JOIN objects f ON f.id=l.from_id "
        "WHERE l.to_id=$1 AND l.type='holds' AND f.type='Agent' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", row["id"])
    if not holders:
        return {"error": f"{seat_id} has no active holder, nothing to vacate"}
    for h in holders:
        await actions.invalidate_link(h["fid"], row["id"], "holds", actor, now)
    await actions.assert_property(row["id"], "vacated_because", because.strip(), actor, now,
                                  _CONF, evidence_class=_EC)
    return {"vacated": seat_id, "was_held_by": [str(h["holder"]) for h in holders]}


# ═══ PEER_OF: a sanctioned pair of verbs minting/healing a symmetric Seat<->Seat bond,
# shaped after retire_seat/vacate_holder immediately above: self-contained, gathers its
# own refusal evidence, writes only once every check clears. Recognition-first
# (research-peer-structures.md, mechanism 11, Ostrom p7): the edge's whole v1 job is
# making a pair legible (to orient(), to the standing-orders peer addendum). The
# two-tier-decision/mutual-hold/disclosure law the research condensed lives in the
# addendum's prose (offices.py), not enforced here; these verbs only mint and heal the
# recognition edge itself.
#
# Symmetric by convention, not by schema: `peer_of` is stored as one directional row
# (from_id, to_id) same as any other link; peer_seats mints it in whichever order the
# caller named seat_a/seat_b, and every reader queries both directions (`_active_peer`,
# the same shape this codebase already uses for a symmetric read on a directional
# column: trigger._managed_edge). No write-time canonical ordering (e.g.
# lexicographically-smaller-first): query-both-directions is the existing idiom, so this
# follows it rather than inventing a second convention.


async def _active_peer(pool: asyncpg.Pool, seat_pk: uuid.UUID) -> asyncpg.Record | None:
    """This seat's current peer_of partner, read in either direction, or None: the one
    query every caller (peer_seats' own precondition, unpeer, peer_of_seat) shares instead
    of hand-rolling the symmetric predicate independently."""
    return await pool.fetchrow(
        "SELECT l.from_id, l.to_id, "
        "CASE WHEN l.from_id=$1 THEN t.canonical ELSE f.canonical END AS peer "
        "FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE l.type='peer_of' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "AND (l.from_id=$1 OR l.to_id=$1) LIMIT 1", seat_pk)


async def peer_of_seat(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """The seat's current peer's canonical id, or None when unpeered/unknown: the shared
    read orient()'s peer block and offices.py's peer addendum both call."""
    row = await pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return None
    peer = await _active_peer(pool, row["id"])
    return peer["peer"] if peer is not None else None


async def peer_reachable(pool: asyncpg.Pool, seat_id: str) -> list[str]:
    """Every seat a search for `seat_id`'s own queue should also cover: the pair faces
    the tree through both peers, scoped to discoverability only (mail delivery itself is
    untouched: widening `_addressed_to_me`'s own resolution is a correctness-sensitive
    change to a path every seat's mail already depends on, not this to make
    unilaterally). Returns [seat_id] alone when unpeered/unknown (never refuses: an
    unpaired seat's queue is still exactly one seat, its own), or [seat_id, peer] when an
    active peer_of bond exists, so a caller reading "whose queue is this" (a future
    review-assignment surface, still missing) checks both names instead of silently
    missing the peer's half. `seat_id` need not itself resolve to a real Seat: the
    ordinary [seat_id] fallback still answers, same as an unknown seat has no peer
    rather than an error, for a pure-read helper with nothing to refuse."""
    peer = await peer_of_seat(pool, seat_id)
    return [seat_id, peer] if peer is not None else [seat_id]


async def peer_ledger(pool: asyncpg.Pool, seat_a: str, seat_b: str) -> list[dict[str, Any]]:
    """The pair's shared reciprocity ledger: a deliberately unsettled reciprocity ledger,
    in the hxaro sense, where open items are what make a parked pair resumable. Zero new
    storage: an item staying open on purpose, resumable rather than closed, is already
    exactly what a Thread's own lifecycle models. open_thread/resolve_thread stay the
    only write path, this only reads. Every open thread owned by either seat, oldest
    first (age is what makes an item worth resuming, not amount): a pair's whole shared
    backlog as one list, which nothing exposed before this (an ordinary `owner` read only
    ever answers for one name). `seat_a`/`seat_b` need not be an active peer_of pair: a
    healed bond's own ledger stays readable, the same as any other historical query in
    this file."""
    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS kind, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS owner "
        "FROM objects o "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "  AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    = 'open' "
        "  AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    = ANY($1) "
        "ORDER BY o.created_at ASC", [seat_a, seat_b])
    return [{"id": str(r["id"])[:8], "summary": r["summary"], "kind": r["kind"],
            "owner": r["owner"], "opened": r["created_at"].isoformat()} for r in rows]


async def _resolve_active_seat(pool: asyncpg.Pool, ref: str) -> asyncpg.Record | None:
    """A Seat by canonical, by its `handle` property, or by raw object id: one lookup
    shared by attach_seat/detach_seat/peer_seats/unpeer/hold_action. Each of these five
    carried an identical exact `canonical=$1`-only lookup, narrower than `rebind_seat`'s
    own resolver (mounts.py) which already accepts all three forms, the same defect
    specimen `attach_seat` was filed under (a resolver-format gap dressed as a
    doesn't-exist error) reproduced four more times rather than fixed once. Returns
    `{id, canonical}` or None; never ambiguous: canonical and handle are each unique
    among active Seats, and a raw id is exact by definition."""
    ref = (ref or "").strip()
    row = await pool.fetchrow(
        "SELECT o.id, o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND (o.canonical=$1 OR EXISTS (SELECT 1 FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='handle' AND a.value #>> '{}' = $1))", ref)
    if row is not None:
        return row
    try:
        oid = uuid.UUID(ref)
    except (ValueError, AttributeError):
        return None
    return await pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE type='Seat' AND status='active' "
        "AND id=$1", oid)




async def peer_seats(
    actions: Actions, seat_a: str, seat_b: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """Mint a symmetric peer_of bond between two active Seats. Not self-scoped: neither
    seat need be the caller's own; `actor` is recorded only as whoever made the bond,
    never a party to it by default. `because` is kept on the edge itself (create_link's
    own `properties`), never stamped asymmetrically on one side of a symmetric
    relationship.

    Refuses loudly on: blank `because`; an unknown/inactive seat on either side;
    seat_a==seat_b; or either seat already carrying an active peer_of edge: v1 is pairs
    only, no chains (a triad is deferred to a later version, after the first pair
    survives contact)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required: peering two seats is a deliberate act on "
                         "the record"}
    seat_a = (seat_a or "").strip()
    seat_b = (seat_b or "").strip()
    async with _peer_lock(actions.pool, seat_a, seat_b):
        row_a = await _resolve_active_seat(actions.pool, seat_a)
        if row_a is None:
            return {"error": f"no such active seat: {seat_a!r}"}
        row_b = await _resolve_active_seat(actions.pool, seat_b)
        if row_b is None:
            return {"error": f"no such active seat: {seat_b!r}"}
        if row_a["id"] == row_b["id"]:
            return {"error": f"{row_a['canonical']} cannot be peered with itself"}
        existing_a = await _active_peer(actions.pool, row_a["id"])
        if existing_a is not None:
            return {"error": f"{row_a['canonical']} already has a peer "
                             f"({existing_a['peer']}), v1 is pairs only, no chains"}
        existing_b = await _active_peer(actions.pool, row_b["id"])
        if existing_b is not None:
            return {"error": f"{row_b['canonical']} already has a peer "
                             f"({existing_b['peer']}), v1 is pairs only, no chains"}
        now = datetime.now(UTC)
        await actions.create_link(row_a["id"], row_b["id"], "peer_of", actor, now, _CONF,
                                  properties={"because": because}, evidence_class=_EC)
        return {"peered": [row_a["canonical"], row_b["canonical"]], "because": because}


async def unpeer(
    actions: Actions, seat_a: str, seat_b: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """Invalidate an active peer_of bond: the compensating-event complement to
    peer_seats. Direction-agnostic: the bond is symmetric, so unpeer(a, b) and unpeer(b, a)
    heal the same edge. `because` is stamped on both seats (`unpeer_because`) rather than
    picking one side arbitrarily, the same reasoning that keeps peer_seats' own `because`
    off any single seat's property set.

    Refuses loudly on: blank `because`; an unknown/inactive seat on either side; or no
    active peer_of edge between the named pair."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required: unpeering two seats is a deliberate act "
                         "on the record"}
    row_a = await _resolve_active_seat(actions.pool, seat_a)
    if row_a is None:
        return {"error": f"no such active seat: {seat_a!r}"}
    row_b = await _resolve_active_seat(actions.pool, seat_b)
    if row_b is None:
        return {"error": f"no such active seat: {seat_b!r}"}
    link = await actions.pool.fetchrow(
        "SELECT from_id, to_id FROM links WHERE type='peer_of' "
        "AND (valid_until IS NULL OR valid_until > now()) "
        "AND ((from_id=$1 AND to_id=$2) OR (from_id=$2 AND to_id=$1))",
        row_a["id"], row_b["id"])
    if link is None:
        return {"error": f"{row_a['canonical']} and {row_b['canonical']} are not peered"}
    now = datetime.now(UTC)
    await actions.invalidate_link(link["from_id"], link["to_id"], "peer_of", actor, now)
    for oid in (link["from_id"], link["to_id"]):
        await actions.assert_property(oid, "unpeer_because", because, actor, now, _CONF,
                                      evidence_class=_EC)
    return {"unpeered": [row_a["canonical"], row_b["canonical"]], "because": because}


async def hold_action(
    actions: Actions, holder: str, held: str, *, act: str, because: str, hours: float = 24,
    actor: str,
) -> dict[str, Any]:
    """Mint a mutual hold: either peer may say hold on the other's specific irreversible
    act, time-boxed. Reuses `open_thread`'s existing Thread shape wholesale (no new
    object type, no new table): the hold is an obligation Thread, `owner=held` (whose
    move it is to respond), `severity='hold'` so it's filterable without a text match,
    resolved the ordinary way: `resolve_thread` on the returned id, same verb every
    other obligation uses, no new resolve path needed. The escalation half (an
    unresolved hold auto-reaching the operator past its time-box) is deliberately not
    built here: that needs a live sweep (pit_watch.py's own shape) or a lint check, a
    separate call not yet made; this only records the hold and its deadline honestly, as
    testimony a human, or later a sweep, can read.

    Refuses loudly on: blank `act`/`because`; `holder==held`; an unknown/inactive seat on
    either side; `hours<=0`; or holder and held not currently an active peer_of pair: v1's
    hold is a peer's power over its own peer, never an unrelated seat's."""
    act = (act or "").strip()
    because = (because or "").strip()
    if not act:
        return {"error": "act is required: name the specific act being held"}
    if not because:
        return {"error": "because is required: holding a peer's act is a deliberate act "
                         "on the record"}
    if hours <= 0:
        return {"error": "hours must be positive, a hold is time-boxed, never indefinite"}
    holder = (holder or "").strip()
    held = (held or "").strip()
    if holder == held:
        return {"error": f"{holder!r} cannot hold its own act"}
    row_holder = await _resolve_active_seat(actions.pool, holder)
    if row_holder is None:
        return {"error": f"no such active seat: {holder!r}"}
    row_held = await _resolve_active_seat(actions.pool, held)
    if row_held is None:
        return {"error": f"no such active seat: {held!r}"}
    holder, held = row_holder["canonical"], row_held["canonical"]
    peer = await peer_of_seat(actions.pool, holder)
    if peer != held:
        return {"error": f"{holder!r} and {held!r} are not an active peer_of pair, a "
                         "hold is a peer's own power, not a stranger's"}
    now = datetime.now(UTC)
    deadline = now + timedelta(hours=hours)
    from src.orchestrator.capture import open_thread

    thread_id = await open_thread(
        actions, f"HOLD by {holder} on {held}'s act ({act}): {because}",
        kind="obligation", owner=held, severity="hold", source=actor)
    for name, value in (
        ("hold_holder", holder), ("hold_held", held), ("hold_act", act),
        ("hold_because", because), ("hold_deadline", deadline.isoformat()),
    ):
        await actions.assert_property(thread_id, name, value, actor, now, _CONF,
                                      evidence_class=_EC)
    return {"held": str(thread_id), "holder": holder, "held_seat": held, "act": act,
            "deadline": deadline.isoformat(),
            "note": "resolve_thread on this id when it's respected/resolved. The "
                    "auto-escalation half is not built yet"}


async def detach_seat(
    actions: Actions, seat: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """Invalidate an active managed_by edge: the compensating-event complement to
    whatever minted it (a cross-house adoption, an office's default binding), and the
    toolkit hole this file used to have: `unpeer` heals peer_of, but nothing healed
    managed_by before this, so the only path was raw SQL. A coordinator is defined by
    having no manager (`derive_role`: 'worker' if a manager exists else 'coordinator'):
    this is a removal, never a repoint, because repointing a detach onto a new manager is
    a different act (whatever mints the replacement edge does that, not this).

    Refuses loudly on: blank `because`; an unknown/inactive seat; or no active managed_by
    edge out of it (nothing to detach)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required: detaching a seat from its manager is a "
                         "deliberate act on the record"}
    row = await _resolve_active_seat(actions.pool, seat)
    if row is None:
        return {"error": f"no such active seat: {seat!r}"}
    link = await actions.pool.fetchrow(
        "SELECT l.from_id, l.to_id, t.canonical AS manager FROM links l "
        "JOIN objects t ON t.id=l.to_id WHERE l.from_id=$1 AND l.type='managed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", row["id"])
    if link is None:
        return {"error": f"{row['canonical']} has no active manager, nothing to detach"}
    now = datetime.now(UTC)
    await actions.invalidate_link(link["from_id"], link["to_id"], "managed_by", actor, now)
    await actions.assert_property(row["id"], "detached_because", because, actor, now, _CONF,
                                  evidence_class=_EC)
    return {"detached": row["canonical"], "was_managed_by": link["manager"], "because": because}


async def attach_seat(
    actions: Actions, worker: str, manager: str, *, evidence: str, actor: str,
) -> dict[str, Any]:
    """Create a managed_by edge: the mirror of detach_seat, and the other half of a
    toolkit hole this file used to have. One prior change built the way to cut a
    management edge; nothing built the way to make one except mint_seat's own
    birth-time create_link (mintseat.py, fires once, only at minting) and fold_seat's
    re-point (only for an existing edge). Every seat that predates mint_seat, was
    adopted, or lost its edge to a detach nobody re-pointed has had no path back except
    raw SQL, the graph's own defect report this house refuses to write around (raw SQL
    against the kernel is a missing verb, never a shortcut). Confirmed live: 30 active
    seats, 23 with no managed_by edge at all; a manager was reported to hold eight of
    them by word alone, but the graph, before this, could represent only two.

    Refuses loudly on: blank `evidence`; either seat unknown/inactive; `worker == manager`
    (a seat cannot manage itself); or an already-active managed_by edge out of `worker`:
    this is a create, never a silent repoint, the same asymmetry detach_seat's own
    docstring draws (repointing onto a new manager is a different act than either half
    alone; detach first, then attach, if that's what's meant)."""
    evidence = (evidence or "").strip()
    if not evidence:
        return {"error": "evidence is required: attaching a seat to a manager is a "
                         "deliberate act on the record"}
    worker_row = await _resolve_active_seat(actions.pool, worker)
    if worker_row is None:
        return {"error": f"no such active seat: {worker!r}"}
    manager_row = await _resolve_active_seat(actions.pool, manager)
    if manager_row is None:
        return {"error": f"no such active seat: {manager!r}"}
    if worker_row["id"] == manager_row["id"]:
        return {"error": f"{worker_row['canonical']} cannot manage itself"}
    existing = await actions.pool.fetchrow(
        "SELECT t.canonical AS manager FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='managed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", worker_row["id"])
    if existing is not None:
        return {"error": f"{worker_row['canonical']} already has an active manager "
                         f"({existing['manager']}), detach_seat first, then attach"}
    now = datetime.now(UTC)
    await actions.create_link(worker_row["id"], manager_row["id"], "managed_by", actor, now,
                              _CONF, evidence_class=_EC)
    await actions.assert_property(worker_row["id"], "attached_evidence", evidence, actor, now,
                                  _CONF, evidence_class=_EC)
    return {"attached": worker_row["canonical"], "now_managed_by": manager_row["canonical"],
            "evidence": evidence}


async def promote_seat(
    actions: Actions, target: str, workers: list[str], *, because: str, actor: str,
) -> dict[str, Any]:
    """A seat can only promote itself over others, never be promoted by another seat on
    its behalf: it has to be self-managed. This verb mints `target` as manager over each
    named worker, in one transaction, self-scoped to the promoted seat's own body (or the
    operator) so a coordinator can never do this to someone else's team. Per-worker
    outcome, never a whole-call failure: a batch of promotions is a manifest of
    independent bets, not one atomic all-or-nothing (promoting over three workers where
    one already reports to someone else should still bond the other two, not refuse the
    whole call over the one name that needed a human to sort out).

    Each worker's own pair, invalidate an active peer_of bond to `target` (if any), then
    attach `managed_by target`, is atomic (both land or neither does): a crash mid-worker
    must never leave a worker unpeered from `target` with no replacement bond. Reuses
    `attach_seat`'s own guard shape (blank evidence, unknown/inactive seat,
    self-management) inline rather than calling it, because attach_seat opens no
    transaction of its own and this whole batch must share one (actions.atomic() from a
    caller already inside one would nest a second transaction on the same connection,
    which asyncpg permits but this file's own atomic() docstring never promises is safe
    to nest).

    House is never stamped here: derive_house reads the new managed_by chain live, same
    as every other seat. Office reissue and the in-process mount-cache heal are the
    caller's job (src/mcp_server.py's `_seat_impl`): both touch state this function's own
    `actions.pool` cannot reach (a Path write, a module-level dict), and reissue in
    particular must run after this transaction commits, not inside it.

    Refuses the whole call loudly on: blank `because`; an unknown/inactive `target`; or
    an unauthorized `actor` (neither `target`'s own holder nor an operator sentinel).
    Never refuses the whole call over a single worker's own bad reference or existing
    manager: those are per-worker verdicts in the returned `workers` manifest instead:
    'bonded' | 'already-managed' | 'refused: <why>'."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required: promoting a seat over workers is a "
                         "deliberate act on the record"}
    target = (target or "").strip()
    target_row = await _resolve_active_seat(actions.pool, target)
    if target_row is None:
        return {"error": f"no such active seat: {target!r}"}
    target_canonical = str(target_row["canonical"])

    from src.orchestrator.charter import is_operator_actor

    if not await is_operator_actor(actions.pool, actor):
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        if caller_seat_id is None or caller_seat_id != target_canonical:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            return {"error": f"{caller_desc} is not authorized to promote {target_canonical} "
                             "over workers, this runs by the promoted seat's OWN body or "
                             "the operator, never a coordinator acting on another's behalf"}

    manifest: dict[str, str] = {}
    affected: list[str] = [target_canonical]  # target's own office/cache always refreshes
    now = datetime.now(UTC)
    async with actions.atomic() as a:
        for worker in workers:
            worker_ref = (worker or "").strip()
            worker_row = await _resolve_active_seat(a.pool, worker_ref)
            if worker_row is None:
                manifest[worker] = f"refused: no such active seat: {worker_ref!r}"
                continue
            worker_canonical = str(worker_row["canonical"])
            if worker_row["id"] == target_row["id"]:
                manifest[worker] = "refused: cannot promote a seat over itself"
                continue
            existing = await a.pool.fetchrow(
                "SELECT t.canonical AS manager FROM links l JOIN objects t ON t.id=l.to_id "
                "WHERE l.from_id=$1 AND l.type='managed_by' "
                "AND (l.valid_until IS NULL OR l.valid_until > now())", worker_row["id"])
            if existing is not None:
                if existing["manager"] == target_canonical:
                    manifest[worker] = "already-managed"
                    affected.append(worker_canonical)
                else:
                    manifest[worker] = f"refused: already managed by {existing['manager']}"
                continue
            peer = await _active_peer(a.pool, worker_row["id"])
            if peer is not None and peer["peer"] == target_canonical:
                await a.invalidate_link(peer["from_id"], peer["to_id"], "peer_of", actor, now)
                for oid in (peer["from_id"], peer["to_id"]):
                    await a.assert_property(oid, "unpeer_because", f"promoted: {because}",
                                            actor, now, _CONF, evidence_class=_EC)
            await a.create_link(worker_row["id"], target_row["id"], "managed_by", actor, now,
                                _CONF, evidence_class=_EC)
            await a.assert_property(worker_row["id"], "attached_evidence", because, actor, now,
                                    _CONF, evidence_class=_EC)
            manifest[worker] = "bonded"
            affected.append(worker_canonical)

    return {"promoted": target_canonical, "workers": manifest, "affected": affected,
            "because": because}
