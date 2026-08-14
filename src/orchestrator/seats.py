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

# how fresh a bound session's pulse must be to count as LIVE contention (mirrors the fleet's
# 15-minute liveness window everywhere else — resolve_seat, agent_liveness, the roster)
_LIVE_SECS = 900

# NOT AN AUTHORIZATION BOUNDARY, AND CANNOT BE ONE (found live 2026-08-02, decision
# c6a894d7): any agent that can run a shell can present any of these strings to a CLI
# door or by importing this module directly — checked here by plain string equality,
# nothing behind it verifies who is actually calling. What these sentinels DO buy is
# DELIBERATENESS AND ATTRIBUTION: a caller must know and deliberately type one of them,
# and the crossing's own audit stamp (mintseat.py's `source=actor`) records exactly
# which one — a real, useful signal, just not a guarantee that the caller truly is the
# operator. Lived here first as mintseat.py's private cross-house-mint guard; moved up
# (mintseat.py now imports it from here) so derive_house's own house-anchor check
# (ruling b4208fa3, thread 105f3425/bec2e4af) shares the SAME definition — two
# independent notions of "the operator's hand" would drift the moment one changed. A
# real authorization boundary would need the check to live somewhere an agent cannot
# reach at all — a different, larger design, not this set.
_OPERATOR_ACTORS = {"operator", "analyst:operator", "console"}


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


@asynccontextmanager
async def _peer_lock(pool: asyncpg.Pool, *canonicals: str) -> AsyncIterator[None]:
    """Serialize peer_seats per seat — the same advisory-lock discipline as _seat_lock
    (task #76 item 1): peer_seats' own "already has a peer" check and its create_link are
    two round-trips with no lock between them, so two concurrent peer_seats calls sharing a
    seat could both pass the check and each mint an edge, leaving that seat with two active
    peer_of bonds — exactly the chain v1's design (this file's own peer_seats docstring)
    says never happens. Locks EVERY seat named, in SORTED order, so overlapping pairs — a
    peer_seats(A, B) racing a peer_seats(B, C) — always acquire their shared seat (B) in the
    same order and never deadlock against each other."""
    keys = sorted({f"peer:{c}" for c in canonicals})
    async with pool.acquire() as conn:
        for key in keys:
            await conn.execute("SELECT pg_advisory_lock(hashtext($1))", key)
        try:
            yield
        finally:
            for key in reversed(keys):
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


async def _seated_house(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The seat-first half alone, shared by `resolve_project` and
    mcp_server._resolve_project_seat_first: a SEATED agent's project is its seat's DERIVED
    house (held_seat, sourcing from derive_house — ruling ff6148b0) — UNCONDITIONALLY, never
    guessed from cwd. Split out so mount()'s own wrapper can call ONLY this half (its cwd
    guess already came from resolve_identity moments earlier, in the SAME pipeline, and must
    win untouched when this returns None — re-deriving a second, independent cwd guess here
    risks disagreeing with it, e.g. under a test's monkeypatched office root)."""
    seat = await held_seat(pool, agent_id)
    return str(seat["house"]) if seat and seat.get("house") else None


async def resolve_project(
    pool: asyncpg.Pool, agent_id: str, cwd: str | None,
) -> str | None:
    """THE ONE project resolver (ruling 577988ed; hoisted, msg 1888 — the mount/project-
    resolution pollution build): every reader that needs "which project is this agent in"
    AND HAS NO cwd-derived guess of its own already funnels through here — the stop hook's
    four hand-rolled `Path(cwd).name` sites and census.live_bodies used to each re-derive
    their own answer. The live specimen was Thoth's own turn: cwd the bare seat-office
    CONTAINER (~/.osiris/seats), basename-guessed "seats", a phantom project neither fleet()
    nor a mail query should ever see.

    A SEATED agent's project is its seat's derived house (`_seated_house`) — UNCONDITIONALLY.
    This is also how `~/.osiris/seats/<handle>` resolves to the seat's HOUSE, not the handle:
    a caller that has already turned that directory into an agent_id (binding_of_handle,
    same as the stop hook's own identity resolution) gets the real house here, never the
    bare handle.

    An UNSEATED agent falls back to a cwd-derived guess — a `.osiris` pin, else the folder's
    basename — EXCEPT the bare office container itself (offices.is_bare_office_root), which
    refuses (None) rather than mint the "seats" phantom. Deliberately NOT the full
    `resolve_identity`: that also GUESSES a session id by scanning ~/.claude/projects for the
    hottest matching transcript, disk I/O this project-only lookup has no use for (and the
    stop hook's per-turn budget and census's per-pid loop can't afford). mount() does NOT use
    this fallback branch (see `_seated_house`'s own note) — it already has resolve_identity's
    answer and only needs the seated override.

    Takes any connection-like with `.fetchval`/`.fetch` (a bare asyncpg.Connection works fine,
    not only a Pool — the stop hook has no pool of its own, only `asyncpg.connect(DSN)`)."""
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
    """VACANT / OCCUPIED / COLD (occupancy piece B, 9f566244) — one authority for whether a
    Seat has a living body in it, the vitals.py way: computed at READ time from
    links(holds) + agent_mounts, no schema change, no new table. THE ACCEPTANCE CASE: Ptah's
    office once showed four bodies where one lived — a seat with no holder at all must read
    VACANT on its own, never silently absent from a query that only ever asks about agents.

    VACANT — no `holds` link has EVER existed for this seat (mint_seat's own law: a seat is
    furniture until a body sits in it). OCCUPIED — an active holder exists AND is live right
    now. COLD — held, now or in the past, but nobody live this instant: the ordinary
    in-between state of a seat between sessions, never an alarm by itself.

    LIVENESS ITSELF IS mounts.agent_liveness() — NOT A SECOND COPY (Alfred's question via
    Thoth, msg 4394/4405, decision 59b3092c): this used to run its own inline agent_mounts-
    only query, the exact single-source shape the dispatch-listener probe had BEFORE ruling
    70493925 gave it a current_assertions.last_active fallback — a repair carried to one
    reader and never to this one, the week's tenth instance of that pattern and the most
    expensive, because every human-facing occupancy figure (roster(), fleet()) reads through
    here. Delegating makes this MORE PERMISSIVE than before (agent_liveness's freshest-of-
    two-signals can only turn a false "cold" into a true "occupied", never the reverse) —
    the direction was checked against every caller before landing (see commit message).

    `live_secs` is NO LONGER HONORED as a variable window — agent_liveness owns the ONE
    liveness window (LIVENESS_WINDOW_MINUTES) every reader now shares, and a second window
    threaded in here would only grow back into a second copy of the disease this fixes. Kept
    only for signature stability (nothing in this codebase varies it — confirmed by grep
    across src/ and tests/, zero non-default call sites); passing a non-default value raises
    rather than being silently dropped, so a future caller who actually needs a different
    window finds a refusal to design against, not a value that quietly stopped doing anything.

    LINEAGE-AWARE the same way held_seat is: the active holder's liveness is read across its
    whole lineage (base id or any `-<suffix>` generation) by agent_liveness itself, because
    bind_holder/follow_binding keep the holds link on the freshest generation but a mount row
    can still be tagged to an ancestor label mid-succession.

    This IS the read launch() needs for its own idempotency (detect-existing-window before
    spawning, never mint a twin body) and its honest liveness receipt (Ra's requirement,
    53ae1a87: body-exists and can-receive are separate states, each independently
    verifiable) — call it with the seat about to be launched into, before launching."""
    if live_secs != _LIVE_SECS:
        raise ValueError(
            f"seat_occupancy no longer supports a custom live_secs ({live_secs!r}) — "
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


_ROSTER_CAVEATS = (
    "chartered_repos and pin.declared are reported as-is, never certified canonical: minting "
    "a SoftwareProject is cheap and mostly ungated, so a name resolving to a real graph object "
    "proves the object exists, not that it is the current or correct name for a repo — near-"
    "duplicate variants (the bytebye/byebyte history is the live example) can each "
    "independently look valid here. Canonicalizing project names is task #137/#152's lane, "
    "not this verb's — a caller that needs 'which of these names is right' asks there, not here.",
    "pin is read from anchor_cwd's own .osiris only. A seat with a distinct tree_cwd (task "
    "#103's office/tree split) may carry its own, possibly different, .osiris there — this "
    "does not read it, and does not check the two agree.",
    "office_exists is a plain directory-existence check on anchor_cwd, nothing more — it does "
    "not mean the office's CLAUDE.md/charter.md content is actually being loaded by a live "
    "session. That question is separate and, as of ruling on Imhotep's #141 scope (msg 3812, "
    "2026-08-08), the office-content mechanism is mid-migration — a seat can read "
    "office_exists=true with orphaned office content.",
    "live_cwd (the current holder's own agent_mounts row, when occupied) can differ from both "
    "anchor_cwd and tree_cwd with nothing wrong on the launch path — Imhotep's #141 scope found "
    "khnum and sekhmet both bound to a tree_cwd while their live sessions sit at the office cwd. "
    "The three are reported separately on purpose; none is silently treated as 'the' cwd.",
    "pin.triage_bucket only ever looks up repo:<pin.declared> — it inherits every one of "
    "pin's own blind spots above (not certified canonical, anchor_cwd-only) plus one more: "
    "a bucket reflects a single triage snapshot taken once per roster() call, not a live "
    "join, so it can go stale the instant something else in the graph changes after this "
    "call returns.",
    "pin.triage_bucket=='duplicate_suspect' is a PER-OBJECT condition (#102, ruling 8cdf905 "
    "— pure marks, no winner ever picked), not a verdict on the SEAT's own pin: it fires "
    "whenever ANY object shares this one's case-folded basename, even when the seat's own "
    "declared object is the real, populated one and the sibling is an empty, unrelated "
    "phantom (task #152's werner/maat/till/aegis specimens — maat/till/aegis confirmed this "
    "shape live). `pin.duplicate_siblings` (present only on this bucket) lists each "
    "colliding object's own canonical and agent_count SO A READER CAN JUDGE THE COLLISION "
    "THEMSELVES — it never picks a winner among them, it only stops attributing a sibling's "
    "emptiness to a seat whose own pin may be perfectly fine.",
    "pin.triage_bucket=='no-such-project' can carry `pin.name_resolution` — DIAGNOSTIC ONLY, "
    "never a resolution. A seat's `project` pin is the CANONICAL SUFFIX by system-wide "
    "contract (every mint/lookup path outside this note builds repo:<value> and mints a NEW "
    "object on a mismatch, task #152/#157's own khepri specimen — decisions 126210f0/"
    "23b667d0); if the declared value instead matches some OTHER object's current NAME (a "
    "post-rename display name, not its canonical), that is worth saying but never worth "
    "silently trusting — `resolved_by:'name'` with a `canonical` names what the pin SHOULD "
    "be corrected to; `ambiguous:true` with `candidates` means it matches more than one "
    "object's name and picks none of them (#102's law, generalized). Absence of this field "
    "on a no-such-project pin means the value matches nothing at all — a genuinely dead pin.",
)


def _dir_exists(path: str | None) -> bool | None:
    """A plain sync wrapper so `roster()` (async) never calls a blocking Path method inline
    (ASYNC240) — None when there's no path to check, never a guess."""
    return Path(path).is_dir() if path else None


async def _triage_bucket_map(pool: asyncpg.Pool) -> dict[str, str]:
    """canonical -> bucket for every active SoftwareProject, ONE call shared across every
    row roster() builds (task #158's cross-reference, thread 251443ff — Khnum's read, msg
    3886) rather than a per-seat re-scan. Goes through `compositions.run_spec`, the same
    public entrypoint mcp_server.py's own `triage()` tool uses — never the module's private
    `_fn_triage`/`_triage_buckets` directly.

    FUNCTION-LOCAL IMPORT, DELIBERATELY (Khnum's read): compositions.py imports this module
    at ITS OWN top (`_OPERATOR_ACTORS`); a top-of-file import back from here would be a real
    cycle — Python would hit a partially-initialized module on whichever side loads first,
    breaking at process start, not at a call site. By the time this function actually runs,
    mcp_server.py has already finished importing both modules, so this is a plain
    `sys.modules` lookup — the same pattern mailbox.py already leans on ~10 times for this
    exact shape (folds.py/agents.py/seats.py/capture.py, all function-local there).

    58 active SoftwareProject objects exist today (measured live) — comfortably under the
    limit=2000 page this asks for, so this is exhaustive in practice, not a silent
    truncation. If that ever stops being true, the caller (roster) would start seeing
    `no-such-project` for a real, merely-unpaged project — the honest failure mode of a
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
    """Task #152's Build 2 (Thoth's dispatch, msg 4215): the OTHER active SoftwareProject
    objects sharing `canonical`'s own case-folded basename — the exact collision
    `_triage_buckets`' `duplicate_suspect` bucket fires on (compositions.py's `per_object`
    CTE, same fold, reproduced here rather than imported to avoid a real import cycle —
    seats.py is already function-local-importing compositions elsewhere in this same file
    for the identical reason, `_triage_bucket_map`'s own docstring explains it). Each
    sibling's own `agent_count` rides along (the same `works_in`-edge count
    `project_ledger` already reports per project, #102-compliant: this NAMES a fact about
    each object independently, it never compares them to crown one canonical). Only called
    when a bucket is ALREADY `duplicate_suspect` — never a blind per-row cost on the common
    (non-flagged) path."""
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
    """Task #152/#157 arc (decisions 126210f0, 23b667d0): a pin's `project` field is the
    CANONICAL SUFFIX by system-wide contract — every mint/lookup path outside this one
    (register_agent, mint_heir's own `_resolve_or_mint_project`) builds `repo:<value>` and
    treats a mismatch as grounds to MINT A NEW OBJECT, never to resolve by name. So when
    roster()'s own canonical-only lookup already came back `no-such-project`, this asks one
    further, PURELY DIAGNOSTIC question: does this value match some OTHER live object's
    current NAME instead? If so, that is worth SAYING — it is the exact shape of Sekhmet's
    own first #152 khepri mistake (a pin holding a display name instead of a canonical,
    later reverted) — but never worth ACTING on here. This function NEVER changes
    triage_bucket, never implies the pin should be silently rewritten to what it found, and
    never picks among multiple name matches: `_resolve_software_project`'s own
    `AmbiguousProjectRef` (ruling 8cdf905's law, generalized) is surfaced as its own
    distinct state, not swallowed into a false single answer. A canonical MATCH here (the
    resolved object's own canonical equals `repo:<pin_value>`) returns None — roster's own
    lookup would already have found that case, this has nothing new to add."""
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
                 "canonical suffix — a pin should hold the canonical (stable across a "
                 "rename), never a display name; correct the pin to the canonical shown "
                 "here, never to the name it currently holds"),
    }


async def roster(
    pool: asyncpg.Pool, *, repo: str | None = None, live_secs: int = _LIVE_SECS,
) -> dict[str, Any]:
    """THE ROSTER (task #140, Alfred's 2813da48): answers "which seat owns this repo" and
    "is a seat's project pin still pointing at something" FROM THE GRAPH — no `ls
    ~/.osiris/seats/*/.osiris` required. Built because Alfred read mount()'s LIVE-agent list
    as the roster, found every seat in his house cold, and read COLD AS VACANT: he
    misrouted mudra's work to kast's seat and separately declared gestalt ownerless and
    edited it himself, both while the seat offices on disk held the correct answer the
    whole time, one query this function now makes unnecessary to hand-run.

    COLD IS NOT VACANT (the root cause, made structurally impossible to collapse here): each
    row's `occupancy` is `seat_occupancy`'s own vacant/occupied/cold, unchanged — vacant means
    no `holds` link has EVER existed for this seat; cold means held, now or in the past, but
    nobody live this instant; occupied means a live body is in it right now. A caller reading
    this dict cannot mistake the second for the first without discarding a field that is
    right there.

    Per seat: `chartered_repos` (charter_of's own `governs` links — self-declared, graph-
    native) and `pin` (a live read of anchor_cwd's `.osiris`, reusing `read_project_pin`'s own
    three-way declared/found-but-unset/could-not-read distinction rather than collapsing it).
    Neither is certified canonical — see `_ROSTER_CAVEATS`, always returned alongside the
    rows, because a roster that states its own blind spots is worth more than one that
    reports a clean answer over a graph that IS wrong in six known places right now (#137/
    #152, running in parallel). `office_exists` and `live_cwd` are separate, deliberately
    uncollapsed axes — see the caveats for exactly what each does and does not mean.

    `pin.triage_bucket` (task #158's cross-reference, thread 251443ff) is a THIRD state, not
    two: `None` when there's nothing declared to look up (`pin.state` isn't `declared`);
    `"no-such-project"` when a project IS declared but no active SoftwareProject object
    named `repo:<pin.declared>` exists in the graph (a dangling pin, not a triage verdict);
    otherwise the real bucket triage's own buckets mode computed for that object —
    `contradicted`/`duplicate_suspect`/`bulk_import`/`orphan`/`hub`/`stale`/`thin`/`normal`.
    Reuses triage verbatim (via `_triage_bucket_map`) rather than inventing a second
    project-health notion — the operator's own complaint ("triage didn't help at all here")
    was a discoverability gap, not a missing capability (5c3dbce5, three weeks unpaid).

    `repo=None` returns every active seat's row. `repo=<name>` instead answers Alfred's exact
    question — "who owns this" — by checking BOTH signals independently: a seat whose charter
    OR current pin names `repo` is a match, tagged with WHICH signal(s) found it. Two seats
    matching from different signals is `agreement="conflict"`, returned as two rows, never
    silently resolved to one. Zero matches is `agreement="no-match"` — explicitly NOT the same
    claim as "nobody owns this repo" (ruling 60bc15db, the third state): it means neither
    signal this function reads found an owner, which is the honest boundary of what a
    graph+pin read can say, not a verdict on the repo's actual ownership."""
    from src.orchestrator.agents import read_project_pin
    from src.orchestrator.charter import charter_of

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
        anchor = facts["anchor_cwd"]
        pin = read_project_pin(anchor)
        if pin.error:
            pin_state = "unreadable"
        elif anchor is None:
            pin_state = "no-office"
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
        rows.append({
            "seat": seat_id, "handle": facts["handle"], "house": facts["house"],
            "occupancy": occ["state"], "holder": occ["holder"],
            "anchor_cwd": anchor, "tree_cwd": facts["tree_cwd"], "live_cwd": live_cwd,
            "office_exists": _dir_exists(anchor),
            "chartered_repos": chartered,
            "pin": {"declared": pin.value, "state": pin_state, "path": pin.path,
                    "error": pin.error, "triage_bucket": triage_bucket,
                    **({"duplicate_siblings": duplicate_siblings}
                       if duplicate_siblings is not None else {}),
                    **({"name_resolution": name_resolution}
                       if name_resolution is not None else {})},
        })

    if repo is None:
        return {"seats": rows, "caveats": list(_ROSTER_CAVEATS)}

    name = repo.removeprefix("repo:").strip()
    matches = [
        {"seat": r["seat"], "handle": r["handle"], "occupancy": r["occupancy"],
         "holder": r["holder"],
         "via": [v for v, hit in (("charter", name in r["chartered_repos"]),
                                  ("pin", r["pin"]["declared"] == name)) if hit]}
        for r in rows
        if name in r["chartered_repos"] or r["pin"]["declared"] == name
    ]
    if not matches:
        agreement = "no-match"
    elif len(matches) == 1:
        agreement = "single-match"
    else:
        agreement = "conflict"
    caveats = list(_ROSTER_CAVEATS)
    if agreement == "no-match":
        caveats.append(
            "no-match means neither a seat's charter nor its current pin names this repo — "
            "not that the repo has no owner. It may be owned by a seat whose pin this "
            "function cannot read, chartered under a name this repo string doesn't exactly "
            "match, or simply not yet declared anywhere this function looks.")
    return {"repo": name, "matches": matches, "agreement": agreement, "caveats": caveats}


# A CASE-FOLDED DENY-LIST, NOT A DETECTOR (Sekhmet's own list, msg 3906 — 8 project-string
# values she found in a fresh agent-project-distribution sweep that look like deliberate
# test/security-research fixtures, not real fleet work): a phantom_verdict must name this
# class explicitly rather than either scoring it a false phantom or silently dropping it
# from the report (ruling 60bc15db's third state, applied to a list this time, not a field).
_KNOWN_TEST_FIXTURE_PROJECTS = frozenset({
    "cc-test-auto", "v218-evil-repo", "rce-disclosure-kit", "cc-clean", "rc-test",
    "nonexistent-probe", "smoketest", "resume_probe",
})

# THE OPERATIONALIZATION OF "NAME-SHAPE", SEKHMET'S JUDGMENT (msg 3906, thread 3892/3900):
# her actual test was eyeballing whether a project's name reads as a directory basename
# (generic, filesystem-y) versus a deliberately chosen one — a call she made by hand against
# the ACTUAL originating cwd. That cwd is usually long gone by the time a phantom is this
# report's business (agent_mounts is a live/recent registry, not history — measured live,
# 32 distinct cwd today against thousands of historical agents), so the only honest
# mechanical stand-in left is this explicit, editable list of generic path-segment names.
# IT WILL ALWAYS BE INCOMPLETE — a reader who disagrees with an entry, or is missing one,
# is disagreeing with THIS LIST, not with some hidden ground truth (requirement #3, msg
# 3900: "phantom is a judgement, so make it one... let a reader disagree with the
# definition").
_GENERIC_PATH_BASENAMES = frozenset({
    "seats", "code", "tmp", "temp", "src", "repo", "repos", "workspace", "projects",
    "project", "worktrees", "worktree", "container", "root", "home", "office", "scratch",
})

_TREE_LEDGER_CAVEATS = (
    "project_ledger covers every ACTIVE SoftwareProject (58 today, measured) — durable "
    "graph state — but by construction can only see a phantom that accumulated at least "
    "one works_in edge; a tree that mounted, produced nothing, and left is invisible to "
    "this whole class of instrument, not just this one field.",
    "live_cwd_ledger's own non-durability caveat (agent_mounts is a live/recent registry, "
    "not history) is stated in ITS OWN section, not only here (Thoth's instruction, msg "
    "3920 — 'nobody reads the bottom'); see its `note` field, not just this list.",
    "phantom_verdict is a JUDGMENT, not a proof, ported from Sekhmet's own rule (msg 3906) "
    "— 'declared' trusts any seat's pin OR a Seat-origin governs edge, never an Agent-"
    "origin one (thread 20af2c95's succession-leak class is evidence FOR phantom, not "
    "against it). project_ledger's own `phantom_verdict_basis`/`note` fields carry the "
    "editable basename list and the mechanical-vs-hand-judgment distinction directly — see "
    "those, not just this line.",
    "the pin's own seat/house/kind fields (Imhotep's schema addition, ruling 719ed5b1) read "
    "almost universally absent as of this build — his migration/writer is still landing "
    "(msg 3909/3915). That is the honest starting state, not a defect in this report's "
    "first pass; nothing here consumes those fields for a verdict yet, matching his own "
    "note that nothing else does either.",
    "READ-ONLY: this reports disagreements and phantom suspicions, it never repairs, folds, "
    "or merges anything — repo:code's disposition is Sekhmet's design, not this verb's.",
    "A GAP IN THE CANONICAL READER ITSELF, FLAGGED NOT FIXED (Thoth's catch, msg 3928): "
    "`_read_osiris_key` (agents.py, used unmodified by this report and by every other "
    "caller — roster(), resolve_identity, project_identity_evidence) climbs `cwd` and its "
    "parents for `.osiris` but never checks whether `cwd` ITSELF exists, so a query against "
    "a DELETED directory silently returns whatever an ancestor's pin says instead — this "
    "instrument works around it locally (`directory_exists`, checked before trusting the "
    "pin) but the canonical reader's own three-state model (OsirisKeyRead) is still missing "
    "a fourth state for this, everywhere else it's called too.",
)


async def _declared_project_index(pool: asyncpg.Pool) -> dict[str, list[str]]:
    """Every project name ANY active seat's pin or Seat-origin `governs` edge (charter_of,
    already Seat-origin-only — an Agent-origin governs edge never reaches this index)
    currently declares, keyed to the seat(s) declaring it. THE fleet-wide half of Sekhmet's
    phantom test (msg 3906): a candidate is legitimate if SOME seat's own declaration
    claims it, not only the tree currently under judgment — Alfred's own calibration case
    (a basename-shaped handle, 'alfred', made legitimate purely by his own deliberate pin).
    Reuses roster() rather than re-querying pins/charters a second way."""
    ros = await roster(pool)
    index: dict[str, list[str]] = {}
    for row in ros["seats"]:
        for name in row["chartered_repos"]:
            index.setdefault(name, []).append(row["seat"])
        if row["pin"]["state"] == "declared" and row["pin"]["declared"]:
            index.setdefault(row["pin"]["declared"], []).append(row["seat"])
    return index


def _phantom_verdict(name: str, declared_by: list[str]) -> str:
    """Sekhmet's rule (msg 3906), in order: a known test fixture is named as one, never
    silently scored either way; a project ANY seat's own declaration claims is `declared`
    regardless of how generic its name looks (pin/charter overrides name-shape, not the
    other way — her Alfred calibration); failing that, a name matching
    `_GENERIC_PATH_BASENAMES` is a `phantom-suspect`; anything else is `undetermined` — a
    real disagreement worth a human's eye, not a confident phantom call either way."""
    if name.lower() in _KNOWN_TEST_FIXTURE_PROJECTS:
        return "test-fixture"
    if declared_by:
        return "declared"
    if name.lower() in _GENERIC_PATH_BASENAMES:
        return "phantom-suspect"
    return "undetermined"


async def project_ledger(pool: asyncpg.Pool, *, limit: int = 200, offset: int = 0,
                         ) -> dict[str, Any]:
    """THE DURABLE HALF of the pin-vs-graph disagreement report (task #158's instrument,
    dispatched msg 3900 off Sekhmet's live repo:seats/repo:code catch, rulings 719ed5b1/
    13af22fc): every ACTIVE SoftwareProject, cross-checked against the fleet-wide declared-
    name index (`_declared_project_index`) and given a `phantom_verdict`
    (`_phantom_verdict`) — never a repair, only a report. `triage_bucket` is reused
    verbatim from the same `_triage_bucket_map` roster()'s pin field already calls, so a
    project's health reads identically whether reached through a seat's pin or through this
    fleet-wide sweep.

    `limit`/`offset` (default 200/0, capped 2000, `total` always reported) — the no-silent-
    caps law, though 58 active SoftwareProjects exist today (measured), comfortably inside
    one page.

    `phantom_verdict_basis` and `note` are returned WITH the rows, not left to a docstring a
    caller may never read (Thoth's own instruction, msg 3920, on the first cut of this
    build): `phantom-suspect` is a MECHANICAL, WEAKER stand-in for Sekhmet's own hand-
    verified name-shape judgment — she looked at the actual originating cwd; this cannot,
    because agent_mounts evicts it long before a phantom accumulates enough content to be
    this report's business (see `live_cwd_ledger`). A reader must be able to tell a
    mechanically-derived suspicion from her verified one at a glance, in the data, not by
    tracing back to this function's source."""
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
                "no seat declares it — it is an operationalization of Sekhmet's own "
                "name-shape test (msg 3906) that survives the originating cwd being long "
                "gone, not a replacement for her looking at one directly. undetermined "
                "means neither test fired — a real disagreement for a human, never a "
                "confirmation the project is legitimate. `declared` IS ALSO NOT A REALNESS "
                "CHECK (Thoth's own catch, msg 3928, live specimens climintworker1/"
                "inferredworker1's own pins declaring cliproj1/soleseathouse — debugging "
                "artifacts, correctly self-declared): this bucket answers 'does some seat's "
                "own declaration claim this name', never 'is this a genuine, intentional "
                "project' — a fake project that correctly declares itself is "
                "indistinguishable from a real one by declaration alone."),
    }


async def live_cwd_ledger(pool: asyncpg.Pool) -> dict[str, Any]:
    """THE LIVE HALF of the pin-vs-graph disagreement report (task #158, dispatch msg 3900):
    every DISTINCT `cwd` `agent_mounts` holds RIGHT NOW (measured live: 32 rows today — an
    ephemeral, recent-sessions registry, never a historical ledger; see `_TREE_LEDGER_
    CAVEATS`) cross-checked against what a fresh mount would resolve there today
    (`resolved_today`: the declared pin, else the basename fallback ruling 13af22fc named,
    refusing at the bare seats container exactly as `resolve_project` already does) versus
    what the graph currently believes (`graph_believes`: live `works_in` targets of every
    agent this cwd's rows name).

    `resolved_today` ANSWERS ONE SPECIFIC QUESTION, NOT "what will this cwd's occupant see
    next" — it is the COLD/BOOTSTRAP resolution (pin, else basename) only. A SEATED agent's
    REAL mount() resolves seat-first (`_seated_house`/`derive_house`) and never consults
    this path at all (roster()'s own docstring: "a SEATED agent's project is its seat's
    derived house — UNCONDITIONALLY"). This field answers "what would an UNKNOWN agent
    resolve to here right now", the exact question 13af22fc's carve-out is about — not a
    prediction for whoever already lives there.

    `directory_exists` is checked FIRST, BEFORE the pin is trusted at all (Thoth's own
    catch, msg 3928, a live 60bc15db specimen IN the instrument built to detect that class):
    `_read_osiris_key` — the canonical reader, used unmodified — climbs `cwd` and every one
    of `cwd.parents` looking for `.osiris`, but never checks whether `cwd` ITSELF exists.
    For a DELETED office (flip68real, resumelanecheck — both real, now-retired Seats whose
    directories are gone), the climb silently lands on the enclosing seats-container's OWN
    pin and reports it as if it belonged to the deleted office — collapsing "this office is
    gone" into "this office exists, pin unset," two conditions with OPPOSITE dispositions
    (one wants a pin written, the other wants the graph's belief reaped). This is a real gap
    in the canonical reader itself, not only this call site — flagged to Thoth, not
    silently patched here; `_read_osiris_key` is unchanged. What IS fixed here is LOCAL:
    `directory_exists=False` short-circuits `pin_state` to `missing-directory` before the
    climb's result is trusted for anything, and `resolved_today` is forced to `None` — "a
    fresh mount would basename-fallback here" is meaningless when nothing can even `cd`
    there (Thoth's own words).

    `agreement`, SIX states now, not five: `ghost` is new — `directory_exists=False` AND the
    graph still believes something. THE SOUL OUTLIVED THE BODY: the office is gone, but
    nothing reconciles the graph's belief — a worse, DIFFERENT finding than a live
    misresolution risk, not a variant of one. Distinct from `graph-only` (the bare seats
    CONTAINER itself — which IS real on disk, `directory_exists=True` — deliberately
    refusing resolution by design, ruling 13af22fc). The other five, unchanged: `no-graph-
    yet` (nothing to cross-check — benign, covers a ghost with no belief either, since
    there's nothing to reconcile), `graph-only`, `match`, `partial-match` (today's
    resolution IS one of the graph's beliefs, but the graph also carries OTHER, likely-
    stale beliefs — worth a look, not urgent), `mismatch` (today's resolution matches NONE
    of the graph's beliefs while the directory IS real — the live misresolution risk)."""
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
        "note": ("NOT A HISTORICAL LEDGER (Thoth's own instruction, msg 3920: this belongs "
                "where a reader hits it, not at the bottom): this section's population is "
                "TODAY's agent_mounts table only, a live/recent registry keyed on job_dir "
                "that EVICTS old rows (measured live: 37 total rows / 32 distinct cwd "
                "against thousands of historical agents). A phantom whose originating "
                "sessions have already ended and been evicted from agent_mounts will NEVER "
                "appear here — only in project_ledger, which reads durable graph state "
                "instead."),
    }


async def tree_ledger(pool: asyncpg.Pool, *, limit: int = 200, offset: int = 0) -> dict[str, Any]:
    """THE PIN-VS-GRAPH DISAGREEMENT REPORT (task #158, Thoth's dispatch msg 3900, off
    Sekhmet's live repo:seats/repo:code catch — rulings 719ed5b1/13af22fc): "the instrument
    that should have found tonight's two phantoms without a human noticing." Read-only,
    fleet-wide, two sections because the durable-history half and the live-right-now half
    genuinely need different populations (see each function's own docstring):

    `project_ledger` — every active SoftwareProject, judged against the fleet's own
    declared-name index, `phantom_verdict` named explicitly (a judgment, not a proof).
    `live_cwd_ledger` — every cwd `agent_mounts` holds right now, cross-checked against
    what a fresh mount resolves there today versus what the graph currently believes.

    `caveats` (`_TREE_LEDGER_CAVEATS`) is always returned alongside both, naming exactly
    what this instrument cannot see (requirement #2, msg 3900) rather than reporting a
    clean sweep over a coverage boundary it never states. NEVER REPAIRS: this names
    disagreements and phantom suspicions; disposing of one (a fold, a rename, a merge) is
    always a separate, deliberate, evidence-gated verb's job, never this one's."""
    projects = await project_ledger(pool, limit=limit, offset=offset)
    cwds = await live_cwd_ledger(pool)
    return {"project_ledger": projects, "live_cwd_ledger": cwds,
            "caveats": list(_TREE_LEDGER_CAVEATS)}


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
    daemon job state (claude_daemon.job_for), never the `reply` injection lane — a distinction
    worth keeping even though ruling 85fba696 sanctioned that lane too: a READ cannot move
    another mind, so this stays side-effect-free by construction, not by policy. Fails
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
    originally promoted here so notify-at-seam (mint_heir's compaction path, thread aeae9977)
    didn't hand-roll a third copy. The stop hook's own former local `_manager_seat` duplicate
    is GONE (msg 1888): the claim that it "cannot import across the script/package boundary
    without a heavier refactor" was STALE — the hook already puts the repo root on sys.path
    and imports `src.orchestrator.mailbox`/`.seats` directly (osiris_stophook.py's own
    `_resolve_worker_identity`); it now calls this function instead of its own copy."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT t.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical=$1 AND l.type='managed_by' "
        "AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)


async def _managed_by_source(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """The `managed_by` edge's OWN source_id for `seat_id`'s active manager link, or None
    when unmanaged — a second read alongside manager_of_seat's (same row) so derive_house
    can tell WHO authorized this specific management relationship, distinct from the bare
    fact of it. Not folded into manager_of_seat itself: that helper has 5 existing callers,
    all treating its return as a bare str | None — widening it would ripple to every one."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT l.source_id FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical=$1 AND l.type='managed_by' "
        "AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)


async def _own_house_stamp(pool: asyncpg.Pool, seat_id: str) -> tuple[str | None, str | None]:
    """(house value, its own source_id) for `seat_id`'s own stored `house` property — one
    read reused both when `seat_id` is a genuine head (no manager at all) and when it's an
    operator-crossed anchor (managed, but the crossing itself keeps its own house)."""
    row = await pool.fetchrow(
        "SELECT a.value #>> '{}' AS house, a.source_id FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='house' "
        "WHERE o.canonical=$1 ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        seat_id)
    return (row["house"], row["source_id"]) if row else (None, None)


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

    THE HOUSE ANCHOR (ruling b4208fa3, thread 105f3425/bec2e4af — the cross-house adoption
    bug that silently annexed Ferryman/halcyon into osiris and, escalated, leaked 50 of
    Thoth's own messages into a hector-vector seat's mailbox): a managed_by edge the
    OPERATOR'S OWN HAND asserted crosses a house boundary DELIBERATELY — mintseat's own
    cross-house-mint guard already refuses that crossing for anyone else. The walk now
    STOPS at any seat whose incoming picture shows the operator's hand on the crossing:
    either the managed_by link TO its manager was itself asserted by an operator actor
    (`_OPERATOR_ACTORS` — the live, empirically-verified signal: today's adoption event
    stamps the LINK with source='operator' even when it never re-touches an ALREADY-
    EXISTING seat's own `house` property, which is exactly what happened to halcyon — its
    house property still carries the source from its original 2026-07-20 mint, an
    unrelated agent id, so checking ONLY the property's own source would silently miss the
    seat that actually caused the mail breach) OR the seat's own `house` property was
    itself asserted by an operator actor (the literal text of the ruling, still checked,
    still true for a seat like Ferryman whose house WAS freshly operator-stamped at mint).
    Either signal makes this seat a house ANCHOR, treated as a head for house purposes even
    while it remains managed — management and habitation are different facts; the org
    chart may cross a boundary without annexing what it crosses.

    Ordinary derivation is UNCHANGED for every seat where neither the operator's own hand
    touched the managed_by edge NOR the house property — an ordinary worker under an
    ordinary manager still walks to its manager exactly as before this fix (decision
    87953278 is the standing witness: Thoth/Khnum/Seshat all still derive 'osiris').

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
            house, _source = await _own_house_stamp(pool, current)
            return house
        link_source = await _managed_by_source(pool, current)
        house, house_source = await _own_house_stamp(pool, current)
        if link_source in _OPERATOR_ACTORS or house_source in _OPERATOR_ACTORS:
            return house  # a house ANCHOR — managed, but the crossing was deliberate
        current = manager
    logger.warning("house derivation for %s exceeded %d hops without reaching a head",
                   seat_id, max_hops)
    return None


async def seat_facts(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any]:
    """A Seat's own handle/house/intended_model/anchor_cwd/tree_cwd, one read — the shared
    resolver mintseat.py and trigger.py each independently hand-rolled as a private
    `_seat_facts` (identical name, near-identical shape, the exact 'two resolvers disagree'
    class this house keeps re-learning). `house` is DERIVED (ruling ff6148b0, decision
    4c9e4bd7), the other four are the seat's own stored assertions — always all five keys
    present (None when absent or the seat doesn't exist), matching trigger.py's stricter
    contract so a caller can index `facts["handle"]` directly rather than every caller
    re-deriving its own tolerant `.get()`.

    `tree_cwd` (task #103's re-scope, ff3bdc37) is DISTINCT from `anchor_cwd` on purpose:
    the office is where identity lives, the tree is where code lives — see `bind_seat_tree`.
    None here means "this seat has no distinct tree" (the common case), never a fallback
    guess; `launch_seat` is the one that decides what None means for a launch."""
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
    """TASK #141 SHAPE 3 (decision eda71c32): a seat that is OCCUPIED right now but has
    never had its `anchor_cwd` captured has no durable trace of its office at all — the
    moment it goes cold, the location is gone from the graph, not merely undeclared. This
    stamps `anchor_cwd` from a live, first-hand observation the instant one exists, so that
    knowledge survives the seat going cold — the same OBSERVATION the fleet-observer
    already records continuously for every other fact it touches (lineage.py), not a
    DECLARATION on anyone else's behalf (Thoth's ruling, msg 4035: a pin written into
    another house's office is a claim by someone else's hand, forbidden cross-house right
    now; an anchor_cwd stamped from what a live session is actually doing is an observation
    recorded, a different act, authorized here).

    NARROW BY DESIGN, matching Thoth's own constraints exactly:
    - NEVER OVERWRITES. A seat with an existing `anchor_cwd` (any value, even stale) is
      skipped outright — there is nothing to arbitrate on this shape, only something
      missing, and a seat that already has an answer is not this function's business.
    - OCCUPIED ONLY. A cold or vacant seat has no live cwd to observe; nothing is guessed
      from history.
    - AMBIGUOUS MEANS NOTHING WRITTEN. If the seat's live holder shows more than one
      DISTINCT cwd across its own fresh `agent_mounts` rows (more than one concurrent
      session, disagreeing), this writes nothing for that seat — a missing value is
      recoverable on a later, cleaner observation; a wrong one is not.

    Returns `stamped` (seat -> the cwd it was given), `skipped_has_anchor`,
    `skipped_not_occupied`, `skipped_ambiguous` — every seat is accounted for in exactly
    one bucket, so a caller can see what happened to all of them, not just the successes."""
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
    """Stamp a seat's REAL attendance signal (thread 96f62338) — replaces ruling `d8a77f80`'s
    broken proxy in `dispatch_dm` ('a seat that manages someone is human-attended'), true only
    while Thoth was the sole manager and false the day workers started minting sub-workers and
    test seats of their own (Imhotep's flip-test mints reclassified him; alfred's #50-pilot
    workers did too — both silently lost their push lane forever). `attended='human'` marks a
    seat the operator actually fronts; `attended='worker'` marks the ordinary case explicitly,
    for reversing a prior stamp. The human-attended guard reads this directly and no longer
    infers anything from managed_by.

    OPERATOR-APPROVED TO CHANGE, ENFORCED (census a5e53ed8/3f97f9c7: this claimed it for
    weeks while any mounted caller could stamp any seat's attendance signal — the same
    unenforced-claim class as rename_seat, fixed the same way, mirroring charter_for's
    already-real check): `actor` must be one of `_OPERATOR_ACTORS`'s sentinels, or the
    seat `actor`'s own lineage holds must BE the target seat's manager (`manager_of_seat`'s
    live `managed_by` edge).

    Refuses LOUDLY on: a value outside {'human','worker'} (no silent typo landing as
    'not human'); a blank `because` (a safety guard reads this property — the reason it
    changed belongs on the record); an unauthorized actor; an unknown or retired seat (a
    Seat's `status` column stays 'active' forever — retirement is the `retired` property
    `retire_seat` stamps, the same signal checked here)."""
    if attended not in _ATTENDED_VALUES:
        return {"error": f"attended must be one of {sorted(_ATTENDED_VALUES)}, not "
                         f"{attended!r}"}
    if not because.strip():
        return {"error": "because is required — a seat's attendance signal gates a safety "
                         "guard (dispatch_dm's human-attended check); the reason it changed "
                         "must be on the record"}
    if actor not in _OPERATOR_ACTORS:
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        manager_seat_id = await manager_of_seat(actions.pool, seat_id)
        if caller_seat_id is None or caller_seat_id != manager_seat_id:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            manager_desc = manager_seat_id or "no manager on record"
            return {"error": f"{caller_desc} is not authorized to set attendance on "
                             f"{seat_id} — its manager is {manager_desc}, and {actor} is "
                             "neither the manager nor the operator"}
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return {"error": f"no such seat: {seat_id!r}"}
    retired = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions a WHERE a.object_id=$1 AND a.name='retired' "
        "AND a.value #>> '{}' = 'true'", row["id"])
    if retired:
        return {"error": f"{seat_id} is retired — cannot stamp attendance on a retired seat"}
    # THE STATUS GAP fix (retire_seat now flips objects.status too, msg 1713) means a
    # retired seat's status is no longer 'active' — the lookup above must not filter on it
    # up front, or a retired seat reads as "no such seat" instead of the specific message
    # above. A merged seat (fold_seat) hits this same non-active branch, pre-existing gap.
    if row["status"] != "active":
        return {"error": f"{seat_id} is {row['status']}, not active — nothing to stamp"}
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
    always with a reason on the record — ENFORCED (census a5e53ed8/3f97f9c7: this claimed
    "a manager or the operator" for weeks while any mounted caller could rename any seat;
    mirrors charter_for's already-real actor-vs-manager_of_seat check).

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
    build: 'vajra' and 'Vajra' must never both be claimable); an unauthorized actor."""
    new_handle = (new_handle or "").strip()
    if not new_handle or len(new_handle) > 40:
        return {"error": "pick a short handle (1-40 chars)"}
    if not because.strip():
        return {"error": "because is required — a rename is testimony; the reason it "
                         "changed must be on the record"}
    if actor not in _OPERATOR_ACTORS:
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        manager_seat_id = await manager_of_seat(actions.pool, seat_id)
        if caller_seat_id is None or caller_seat_id != manager_seat_id:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            manager_desc = manager_seat_id or "no manager on record"
            return {"error": f"{caller_desc} is not authorized to rename {seat_id} — its "
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


async def bind_seat_tree(
    actions: Actions, *, seat_id: str, tree_cwd: str, actor: str, because: str,
) -> dict[str, Any]:
    """Point a seat's CODE checkout at `tree_cwd` — deliberately, distinct from `anchor_cwd`
    (the seat's identity home, untouched by this). Task #103's re-scope (ff3bdc37, accepted
    whole via Thoth DM 2794): "the office is where identity lives, the tree is where code
    lives", and collapsing the two is John's own catastrophe (#128) repeated at seat scope.
    `launch_seat`'s own idempotency reuses whatever is recorded here across every relaunch
    until this is called again — never a launch's own side effect (ff3bdc37's own law: which
    tree a seat is on stays an auditable graph write, same discipline `rename_seat` holds
    for a handle change).

    OSIRIS NEVER PROVISIONS THE TREE (harness owns isolation, ff3bdc37) — this call RECORDS
    a location, it does not create one. `launch_seat` is the one that checks the directory
    actually exists on disk before trusting it; this verb writes unconditionally on a valid
    call, exactly as `ensure_seat`'s own `anchor_cwd` write does.

    OPERATOR-OR-MANAGER ONLY, ENFORCED (found 2026-08-02 while scoping the seat-metadata
    merge, Thoth msg 3307: "nobody has ever ruled who may bind a tree" — this had NO
    authority language at all, claimed or enforced, unlike its rename_seat/set_seat_attended
    siblings which at least overclaimed. Judged gate-worthy, not honesty-worthy, because the
    write is not merely descriptive metadata: `tree_cwd` is what `launch_seat` trusts as the
    CODE a relaunched seat executes — a wrong or hostile rebind is a code-execution vector at
    the seat's next launch, not a cosmetic drift. Mirrors charter_for's already-real
    actor-vs-manager_of_seat check, the same pattern rename_seat/set_seat_attended now carry
    (commit c2020a1)): `actor` must be one of `_OPERATOR_ACTORS`'s sentinels, or the seat
    `actor`'s own lineage holds must BE the target seat's manager (`manager_of_seat`'s live
    `managed_by` edge).

    Refuses LOUDLY on: a blank `tree_cwd`; a blank `because` (a location change is
    testimony — the same discipline `rename_seat`/`set_seat_attended` hold); an unauthorized
    actor; an unknown seat."""
    tree_cwd = (tree_cwd or "").strip()
    if not tree_cwd:
        return {"error": "bind_seat_tree needs a tree_cwd"}
    if not because.strip():
        return {"error": "because is required — a tree binding is testimony; the reason "
                         "it changed must be on the record"}
    if actor not in _OPERATOR_ACTORS:
        caller_seat = await held_seat(actions.pool, actor)
        caller_seat_id = str(caller_seat["seat_id"]) if caller_seat else None
        manager_seat_id = await manager_of_seat(actions.pool, seat_id)
        if caller_seat_id is None or caller_seat_id != manager_seat_id:
            caller_desc = (f"{actor} (seat {caller_seat_id})" if caller_seat_id
                          else f"{actor} (holds no seat)")
            manager_desc = manager_seat_id or "no manager on record"
            return {"error": f"{caller_desc} is not authorized to bind {seat_id}'s tree — "
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
    await actions.assert_property(row["id"], "tree_cwd", tree_cwd, actor, datetime.now(UTC),
                                  _CONF, evidence_class=_EC)
    return {"seat": seat_id, "old_tree_cwd": old_tree, "tree_cwd": tree_cwd,
           "because": because,
           "note": "recorded — osiris never provisions the directory itself; launch_seat "
                   "checks it exists before trusting it"}


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


async def seat_holder_ineligible(pool: asyncpg.Pool, name: str) -> str | None:
    """THE MISSING DISTINCTION `binding_of_handle` cannot make on its own (Thoth's dispatch,
    DM 2360; rulings 1a64ae9a/aee67e6d — John XV/XVI, resolved live). `binding_of_handle`
    collapses FOUR distinct failure shapes into one bare `None`: no such seat, an ambiguous
    handle, a genuinely vacant seat (no active `holds` edge at all), and — THE CASE THAT
    MATTERS HERE — a unique seat WITH an active holder who is marked retired/false_mint (or
    a spawn wearing the handle, Phase C). `resolve_seat` (agents.py, FROZEN tonight — this
    function never touches it) treats that bare None as "try the un-seated-lineage fallback
    instead" for ALL FOUR shapes alike, and the fallback's own assertion-based search finds
    whatever OTHER Agent object still carries a stale `handle` assertion for this name — a
    dead generation, addressed with the confidence of a real resolution (send(to_agent=
    'John') delivered to agent:d5c671c1-xiv, his DEAD PREDECESSOR, while -xv/-xvi were the
    living lineage).

    Returns None for the other three shapes (no seat / ambiguous / genuinely vacant) — the
    fallback is the CORRECT answer for an un-seated or truly-empty lineage, and this
    function must never block it. Returns a REASON STRING, naming the seat and its
    ineligible holder(s), ONLY for the fourth shape: a seat exists, uniquely, DOES have at
    least one active holder, and NONE of them are eligible. A caller (send_message) that
    sees this string must REFUSE before ever calling resolve_seat — the refusal belongs in
    the resolution, not a post-hoc check on the receipt (both `dm_to` and `lineage_head`
    agree on the SAME wrong answer once the fallback has already run, so no receipt-side
    check can catch this after the fact).

    ANY ELIGIBLE HOLDER, NOT JUST THE NEWEST (Thoth's correction to this function's first
    build, DM 2377 — caught in review, HELD the deploy of ddb8104): `binding_of_handle`
    FILTERS OUT marked/visitor holders in its own WHERE clause, THEN takes the newest of
    what remains — so a seat with an OLDER eligible holder still resolves correctly even
    when a NEWER active holds edge belongs to a marked one. The first build of this
    function asked the wrong question ("is the newest holder eligible") instead of the one
    its own docstring already promised ("does any eligible holder exist") — those two
    disagree exactly whenever a Seat carries more than one active `holds` edge with the
    newest marked and an older one still eligible. NOT hypothetical: seat:c476e7a2 carries
    exactly this shape right now (decision 6ce4ac5f) — a zero-turn phantom mint's own
    ordinary seam (gen N+1 takes the newest holds edge, gets marked false_mint seconds
    later, gen N still holds and is still eligible) reproduces it on demand. A
    fleet-wide single point of failure (send_message) must never refuse-to-serve on a check
    that can false-positive (ruling 577988ed) — this now checks the FILTERED (eligible) set
    first, mirroring binding_of_handle exactly, and only names a refusal when that set is
    empty while active holders exist at all."""
    seats = [r["canonical"] for r in await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Seat' AND o.status='active' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1)", name)]
    if len(seats) != 1:
        return None  # no seat, or an ambiguous handle — not this function's shape to name
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
        return None  # a genuinely vacant seat — the un-seated fallback is the right answer
    ineligible = [r for r in rows if r["marked"] or r["visitor"]]
    if len(ineligible) < len(rows):
        return None  # AT LEAST ONE eligible holder — binding_of_handle resolves fine
    names = ", ".join(
        f"{r['canonical']} ({'marked retired/false_mint' if r['marked'] else 'a visitor spawn'})"
        for r in rows)
    why = f"every active holder is ineligible ({names})"
    return (f"{seats[0]} is the unique living seat for {name!r}, but {why} — "
            "no eligible holder exists")


async def _seat_display(pool: asyncpg.Pool, seat_id: str) -> dict[str, Any]:
    """Handle + house for a seat ADDRESS. `house` is DERIVED (ruling ff6148b0, decision
    4c9e4bd7 — reaffirmed as the consolidation target by 1db1ff41's ruling 1), never this
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
    the exact 'legacy write stays inert' shape derive_house already documents).

    PRIOR-ART SURFACED, NEVER REFUSED (obligation e4612853's sibling, ruling 38c71544's
    family): the receipt's own `prior_art`/`prior_art_flag` keys, when present, name a
    standing Decision that may already cover this seat's house — the same search()-based
    guard record_decision runs on itself, generalized here. Cannot distinguish a
    deliberate correction from an uninformed overwrite; only ensures the write does not
    land silently unread."""
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
    from src.orchestrator.capture import property_prior_art

    prior_art_bits = await property_prior_art(
        actions.pool, subject_canonical=seat_id, field="house", new_value=new_house,
        actor=source)
    return {"seat_id": seat_id, "house": new_house, "was": was, **prior_art_bits}


async def resync_seat_house_third_party(
    actions: Actions, seat_id: str, new_house: str, *, source: str, reason: str,
) -> dict[str, Any]:
    """THE THIRD-PARTY SIBLING OF `correct_house` — task #152's khepri/deckard/metron repair
    (decision 6602d39d): `correct_house` is deliberately SELF-scoped (a head correcting its
    OWN identity declaration, ruling 87953278), but a stale Seat.house left behind by a
    `rename_project` that never propagates to the seat itself (ruling 719ed5b1's own five-
    key schema was silent on this exact property — `Seat.house` written once at
    `ensure_seat` mint time, never resynced by anything since) is not the seat's own act to
    make; it is an individually-diagnosed, individually-authorized correction landed BY
    someone else, on the same explicit-reason audit-trail discipline as
    `offices.correct_pin_value`. Refuses on an empty house or an empty reason for the exact
    same cause that function refuses an empty reason: a correction with no stated reason is
    the silent overwrite this house rules against, not a fix. Does NOT check headship or
    caller identity — this is explicitly a third-party act, unlike `correct_house`, and
    callers are responsible for the authorization this docstring cannot enforce."""
    new_house = (new_house or "").strip()
    if not new_house:
        return {"error": "a house needs a name"}
    if not reason.strip():
        return {"error": "a correction with no reason is exactly the silent overwrite "
                         "719ed5b1 rules against — refusing"}
    facts = await seat_facts(actions.pool, seat_id)
    was = facts.get("house")
    if was == new_house:
        return {"written": False, "seat_id": seat_id, "house": was}
    seat_obj = await actions.create_or_find_object("Seat", seat_id, source)
    await actions.assert_property(seat_obj, "house", new_house, source, datetime.now(UTC),
                                  _CONF, evidence_class=_EC)
    return {"written": True, "seat_id": seat_id, "house": new_house, "was": was,
            "reason": reason}


async def _move_seat_estate(
    actions: Actions, dupe_oid: uuid.UUID, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """The seat estate-move itself, factored out of `fold_seat` so `reconcile_seat_fold`
    (#127) can run the EXACT same repair on an already-merged pair rather than a second
    implementation that could drift from what a normal fold already does. Every item here
    is individually idempotent — a holder or managed_by edge already re-pointed, or mail
    already moved, no longer matches its own WHERE clause — so running this twice, or
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
    dupe_oid, into_oid = by_label[dupe]["id"], by_label[into]["id"]
    # THE ESTATE, seat-shaped: active holders move first — the point of this verb, where
    # fold_agent refuses instead (bind_holder's own succession law converges a twin's
    # multiple concurrent holders to the NEWEST one, matching every recency-wins
    # convention elsewhere in this codebase) — mail and managed_by follow too, BEFORE the
    # kernel merge (#59's own precondition fix, mirroring fold_project/fold_agent's now-
    # shared pattern): a crash between here and the merge below leaves dupe.status==
    # 'active', so a retry continues rather than hitting the merge's own "already folded"
    # refusal with estate stranded on dupe forever.
    estate = await _move_seat_estate(actions, dupe_oid, dupe, into, actor)
    # the kernel merge: event, projection, resolve-on-read — the same primitive fold_agent
    # itself calls, type-agnostic, no Agent-only check inside it
    await actions.merge_objects(into_oid, dupe_oid, justification=evidence, actor=actor)
    return {"folded": dupe, "into": into, **estate}


async def reconcile_seat_fold(
    actions: Actions, *, dupe: str, into: str, actor: str,
) -> dict[str, Any]:
    """THE REPAIR PATH fold_seat never had (#127, Thoth's framing verbatim: "folds are
    idempotent-by-REFUSAL when they need to be idempotent-by-REPAIR"): re-points any live
    holder/managed_by/mail estate item still aimed at an ALREADY-merged dupe, using the
    SAME `_move_seat_estate` `fold_seat` itself calls — not a second implementation that
    could drift from what a normal fold already does.

    THE INVERSE PRECONDITION of fold_seat, on purpose, so the two verbs' refusal
    conditions never overlap: fold_seat REQUIRES status=='active' and refuses an already-
    folded dupe; reconcile REQUIRES dupe.status=='merged' AND dupe's own `merged_into`
    pointing at exactly `into` (refuses to redirect a dupe merged into some OTHER seat —
    never guesses which pair a caller means). NEVER re-performs the fold: no
    `merge_objects` call.

    NO ACTOR GATE, matching `fold_seat`'s own current (flagged, unfixed) posture —
    finding 962579a6: this build inherits that asymmetry consistently rather than
    reconciling it now.

    Refuses LOUDLY on: blank dupe/into; dupe==into; dupe not resolving to a Seat;
    dupe.status != 'merged' (fold_seat's job, not this one's); dupe's own `merged_into`
    not equal to `into`'s id; into not resolving to an ACTIVE Seat."""
    dupe, into = (dupe or "").strip(), (into or "").strip()
    if not dupe or not into:
        return {"error": "reconcile_seat_fold needs both labels: dupe and into"}
    if dupe == into:
        return {"error": "dupe and into name the same seat — nothing to reconcile"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Seat'",
        dupe)
    if row is None:
        return {"error": f"no such seat: {dupe!r} — reconcile never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is {row['status']}, not merged — reconcile_seat_fold "
                         "only repairs an ALREADY-completed fold; use merge to fold it in "
                         "the first place"}
    into_row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Seat'", into)
    if into_row is None:
        return {"error": f"no such seat: {into!r} — reconcile never invents a label"}
    if into_row["id"] != row["merged_into"]:
        actual = await actions.pool.fetchval(
            "SELECT canonical FROM objects WHERE id=$1", row["merged_into"])
        return {"error": f"{dupe} is merged into {actual}, not {into} — "
                         "reconcile_seat_fold never redirects to a different pair"}
    if into_row["status"] != "active":
        return {"error": f"{into} is {into_row['status']}, not active"}
    estate = await _move_seat_estate(actions, row["id"], dupe, into, actor)
    return {"reconciled": dupe, "into": into, **estate}


async def unfold_seat(
    actions: Actions, *, dupe: str, because: str, actor: str, execute: bool = False,
) -> dict[str, Any]:
    """Reverse a wrongful fold_seat — the Seat sibling of `folds.unfold_agent`, built for
    PARITY (ruling 31c02dca: fold_seat shipped with NO reversal at all, so a fold here was
    permanent and unrepairable — task #127's own named case). DRY RUN IS THE DEFAULT
    (`execute=False`): returns the plan without writing.

    Refuses LOUDLY (an error dict, nothing written) when: `because` is blank; `dupe` is
    unknown or not currently folded (status != 'merged'); the ORIGINAL fold's own
    justification cites the operator's word and `because` does not ALSO carry a fresh one
    — the same discipline `unfold_agent` already holds, generalized to seats.

    ESTATE, seat-shaped: fold_seat's holders and managed_by moves are event-sourced
    (`invalidate_link` + `create_link`) and are restored automatically WHEN AND ONLY WHEN
    nothing has touched them since (`folds._reversible_moved_links` — a holder now on some
    OTHER seat, or a managed_by edge since re-pointed again, is never guessed back). Mail
    was moved by a raw UPDATE (the same as `fold_agent`'s own mail leg) and is never
    reversible — always reported as `estate_unreturnable`, never restored."""
    from src.orchestrator.folds import _reversible_moved_links

    dupe, because = (dupe or "").strip(), (because or "").strip()
    if not because:
        return {"error": "an unfold without a because is an un-audited reversal — cite "
                         "the evidence/ruling that proves the fold was wrong"}
    if not dupe:
        return {"error": "unfold_seat needs a dupe label"}
    row = await actions.pool.fetchrow(
        "SELECT id, status, merged_into FROM objects WHERE canonical=$1 AND type='Seat'",
        dupe)
    if row is None:
        return {"error": f"unknown seat: {dupe} — an unfold never invents a label"}
    if row["status"] != "merged":
        return {"error": f"{dupe} is not folded (status={row['status']}) — nothing to "
                         "unfold"}
    into_id = row["merged_into"]
    into_canon = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", into_id)
    ev = await actions.pool.fetchrow(
        "SELECT payload, actor, created_at FROM object_events "
        "WHERE event_type='merge' AND related_id=$1 ORDER BY created_at DESC LIMIT 1",
        row["id"])
    original_evidence = str((ev["payload"] or {}).get("justification", "")) if ev else ""
    if "operator" in original_evidence.lower() and "operator" not in because.lower():
        return {"error": f"{dupe}'s fold was justified by citing the operator's word "
                         f"({original_evidence!r}) — an unfold needs the operator's word "
                         "too; add it to `because` or get it first"}

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
            "note": ("pre-fold UPDATEs overwrote to_agent in place — these predate the "
                     "fold and still sit on the living seat, but nothing proves they were "
                     "ever addressed to dupe rather than already into's own; read them "
                     "and judge by hand, never auto-moved") if unreturnable_mail else
                    "none found — no pre-fold mail sits unclaimed on the living seat",
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
        "note": (f"{dupe} is active again — provenance for the folded era stays on the "
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
    """Mark a Seat permanently CLOSED — a genuinely dead role, no successor, no merge
    target (fold_seat is for a twin; this is for a role that's simply over). DISTINCT from
    the session-level retire() (mcp_server.py) — that one retires a live AGENT's own
    turn; this retires the ROLE ITSELF, for every mind that ever might hold it.

    Refuses LOUDLY on: unknown or already-inactive seat; an ACTIVE holder (a live mind
    sitting in a seat is not this verb's business to evict — transfer or let it vacate
    first, the same discipline fold_agent already holds for agents); an active peer_of
    edge (a peered seat retiring first would leave the bond pointing at a dead seat
    forever — unpeer first, same reasoning as the holder guard).

    THE STATUS GAP (Seshat msg 1686, live specimen operator-caught msg 1713): this used
    to stamp only the `retired` PROPERTY, leaving objects.status reading 'active' forever
    — invisible to anything that checks status rather than the property, and nothing
    stopped a fresh claim from re-binding to a seat that LOOKED retired. Now flips both
    layers: the property (for anything already reading it) AND objects.status via
    Actions.set_status (the real compensating event, same pattern as retire_project),
    so a retired seat is actually inert, not just labeled.

    THE PEER GUARD (Khnum IX's review, msg 1774, of the peer_of build): unpeer requires
    BOTH seats active to resolve them, so retiring a peered seat first — nothing else
    stopped that — left the bond stuck active forever, pointing at a dead seat with no
    sanctioned verb able to heal it (the identical class of bug as a fold/bond that
    outlives the object it names). Refusing here, symmetrically with the holder check,
    keeps the fix on the RETIRE side rather than loosening unpeer's own active-seat
    guard, which would weaken a real invariant elsewhere."""
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
    peer = await _active_peer(actions.pool, row["id"])
    if peer is not None:
        return {"error": f"{seat_id} is peered with {peer['peer']} — retiring it would "
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


# ═══ PEER_OF (msg 1770, Thoth's dispatch; ruling d74492ee, spec e6636c7e) — a sanctioned
# pair of verbs minting/healing a SYMMETRIC Seat<->Seat bond, shaped after retire_seat/
# vacate_holder immediately above: self-contained, gathers its own refusal evidence, writes
# only once every check clears. Recognition-first (research-peer-structures.md, mechanism
# 11, Ostrom p7): the edge's whole v1 job is making a pair LEGIBLE (to orient(), to the
# standing-orders PEER ADDENDUM) — the two-tier-decision/mutual-hold/disclosure LAW the
# research condensed lives in the addendum's prose (offices.py), not enforced here; these
# verbs only mint and heal the recognition edge itself.
#
# SYMMETRIC BY CONVENTION, NOT BY SCHEMA: `peer_of` is stored as one directional row
# (from_id, to_id) same as any other link; peer_seats mints it in whichever order the caller
# named seat_a/seat_b, and every reader queries BOTH directions (`_active_peer`, the same
# shape this codebase already uses for a symmetric read on a directional column —
# trigger._managed_edge). No write-time canonical ordering (e.g. lexicographically-smaller-
# first) — query-both-directions is the existing idiom, so this follows it rather than
# inventing a second convention.


async def _active_peer(pool: asyncpg.Pool, seat_pk: uuid.UUID) -> asyncpg.Record | None:
    """This seat's current peer_of partner, read in EITHER direction, or None — the one
    query every caller (peer_seats' own precondition, unpeer, peer_of_seat) shares instead
    of hand-rolling the symmetric predicate independently."""
    return await pool.fetchrow(
        "SELECT l.from_id, l.to_id, "
        "CASE WHEN l.from_id=$1 THEN t.canonical ELSE f.canonical END AS peer "
        "FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE l.type='peer_of' AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "AND (l.from_id=$1 OR l.to_id=$1) LIMIT 1", seat_pk)


async def peer_of_seat(pool: asyncpg.Pool, seat_id: str) -> str | None:
    """The seat's current peer's canonical id, or None when unpeered/unknown — the shared
    read orient()'s peer block and offices.py's PEER ADDENDUM both call."""
    row = await pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
    if row is None:
        return None
    peer = await _active_peer(pool, row["id"])
    return peer["peer"] if peer is not None else None


async def peer_reachable(pool: asyncpg.Pool, seat_id: str) -> list[str]:
    """Every seat a search for `seat_id`'s own queue should ALSO cover — task #76 item 5b
    (spec e6636c7e: "the pair faces the tree through BOTH peers"), scoped to
    DISCOVERABILITY ONLY per Thoth's own ruling (msg 4337/4351): mail delivery itself is
    untouched — widening `_addressed_to_me`'s own resolution is a correctness-sensitive
    change to a path every seat's mail already depends on, not this item's to make
    unilaterally. Returns [seat_id] alone when unpeered/unknown (never refuses — a
    stranger's queue is still exactly one seat, its own), or [seat_id, peer] when an active
    peer_of bond exists — so a caller reading "whose queue is this" (a future review-
    assignment surface, item 5c's own still-missing piece) checks both names instead of
    silently missing the peer's half. `seat_id` need not itself resolve to a real Seat —
    the ordinary [seat_id] fallback still answers, same as an unknown seat has no peer
    rather than an error, for a pure-read helper with nothing to refuse."""
    peer = await peer_of_seat(pool, seat_id)
    return [seat_id, peer] if peer is not None else [seat_id]


async def peer_ledger(pool: asyncpg.Pool, seat_a: str, seat_b: str) -> list[dict[str, Any]]:
    """The pair's shared reciprocity ledger (task #76 item 3, spec e6636c7e's "deliberately
    UNSETTLED reciprocity ledger" — hxaro: open items are what make a parked pair
    resumable). ZERO new storage: an item staying open on purpose, resumable rather than
    closed, is already exactly what a Thread's own lifecycle models — open_thread/
    resolve_thread stay the only write path, this only reads. Every OPEN thread owned by
    EITHER seat, oldest first (age is what makes an item worth resuming, hxaro's own
    framing, not amount) — a pair's whole shared backlog as one list, which nothing
    exposed before this (an ordinary `owner` read only ever answers for ONE name).
    `seat_a`/`seat_b` need not be an active peer_of pair — a healed bond's own ledger stays
    readable, the same as any other historical query in this file."""
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


async def peer_seats(
    actions: Actions, seat_a: str, seat_b: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """Mint a symmetric peer_of bond between two active Seats. NOT self-scoped — neither
    seat need be the caller's own; `actor` is recorded only as whoever made the bond, never
    a party to it by default. `because` is kept on the EDGE itself (create_link's own
    `properties`), never stamped asymmetrically on one side of a symmetric relationship.

    Refuses LOUDLY on: blank `because`; an unknown/inactive seat on either side;
    seat_a==seat_b; or either seat already carrying an active peer_of edge — v1 is PAIRS
    ONLY, no chains (a triad is deferred to v1.1, after the first pair survives contact)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — peering two seats is a deliberate act on "
                         "the record"}
    seat_a = (seat_a or "").strip()
    seat_b = (seat_b or "").strip()
    async with _peer_lock(actions.pool, seat_a, seat_b):
        row_a = await actions.pool.fetchrow(
            "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
            "AND status='active'", seat_a)
        if row_a is None:
            return {"error": f"no such active seat: {seat_a!r}"}
        row_b = await actions.pool.fetchrow(
            "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
            "AND status='active'", seat_b)
        if row_b is None:
            return {"error": f"no such active seat: {seat_b!r}"}
        if row_a["id"] == row_b["id"]:
            return {"error": f"{row_a['canonical']} cannot be peered with itself"}
        existing_a = await _active_peer(actions.pool, row_a["id"])
        if existing_a is not None:
            return {"error": f"{row_a['canonical']} already has a peer "
                             f"({existing_a['peer']}) — v1 is pairs only, no chains"}
        existing_b = await _active_peer(actions.pool, row_b["id"])
        if existing_b is not None:
            return {"error": f"{row_b['canonical']} already has a peer "
                             f"({existing_b['peer']}) — v1 is pairs only, no chains"}
        now = datetime.now(UTC)
        await actions.create_link(row_a["id"], row_b["id"], "peer_of", actor, now, _CONF,
                                  properties={"because": because}, evidence_class=_EC)
        return {"peered": [row_a["canonical"], row_b["canonical"]], "because": because}


async def unpeer(
    actions: Actions, seat_a: str, seat_b: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """Invalidate an active peer_of bond — the compensating-event complement to
    peer_seats. Direction-agnostic: the bond is symmetric, so unpeer(a, b) and unpeer(b, a)
    heal the same edge. `because` is stamped on BOTH seats (`unpeer_because`) rather than
    picking one side arbitrarily — the same reasoning that keeps peer_seats' own `because`
    off any single seat's property set.

    Refuses LOUDLY on: blank `because`; an unknown/inactive seat on either side; or no
    active peer_of edge between the named pair."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — unpeering two seats is a deliberate act "
                         "on the record"}
    row_a = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", (seat_a or "").strip())
    if row_a is None:
        return {"error": f"no such active seat: {seat_a!r}"}
    row_b = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", (seat_b or "").strip())
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
    """Mint a mutual HOLD — task #76 item 4a (spec e6636c7e): either peer may say HOLD on
    the OTHER's specific irreversible act, time-boxed. Reuses `open_thread`'s existing
    Thread shape wholesale (no new object type, no new table): the hold IS an obligation
    Thread, `owner=held` (whose move it is to respond), `severity='hold'` so it's filterable
    without a text match, resolved the ordinary way — `resolve_thread` on the returned id,
    same verb every other obligation uses, no new resolve path needed. The escalation half
    of the spec (an unresolved hold auto-reaching the operator past its time-box) is
    DELIBERATELY NOT built here — that needs a live sweep (pit_watch.py's own shape) or a
    lint check, a separate call Thoth hasn't made yet (decision e85d3040); this only records
    the hold and its deadline honestly, as testimony a mind — or, later, a sweep — can read.

    Refuses LOUDLY on: blank `act`/`because`; `holder==held`; an unknown/inactive seat on
    either side; `hours<=0`; or holder and held not currently an active peer_of pair — v1's
    hold is a PEER'S power over its OWN peer, never a stranger's."""
    act = (act or "").strip()
    because = (because or "").strip()
    if not act:
        return {"error": "act is required — name the specific act being held"}
    if not because:
        return {"error": "because is required — holding a peer's act is a deliberate act "
                         "on the record"}
    if hours <= 0:
        return {"error": "hours must be positive — a hold is time-boxed, never indefinite"}
    holder = (holder or "").strip()
    held = (held or "").strip()
    if holder == held:
        return {"error": f"{holder!r} cannot hold its own act"}
    row_holder = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", holder)
    if row_holder is None:
        return {"error": f"no such active seat: {holder!r}"}
    row_held = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", held)
    if row_held is None:
        return {"error": f"no such active seat: {held!r}"}
    peer = await peer_of_seat(actions.pool, holder)
    if peer != held:
        return {"error": f"{holder!r} and {held!r} are not an active peer_of pair — a "
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
            "note": "resolve_thread on this id when it's respected/resolved — the "
                    "auto-escalation half is not built yet (decision e85d3040)"}


async def detach_seat(
    actions: Actions, seat: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """Invalidate an active managed_by edge — the compensating-event complement to whatever
    minted it (a cross-house adoption, an office ceremony's default binding), and the toolkit
    hole named at thread fad0dc14: `unpeer` heals peer_of, but nothing healed managed_by
    before this, so the only path was raw SQL. A COORDINATOR IS DEFINED BY HAVING NO MANAGER
    (`derive_role`: 'worker' if a manager exists else 'coordinator') — this is a REMOVAL,
    never a repoint, because repointing a detach onto a new manager is a DIFFERENT act
    (whatever mints the replacement edge does that, not this).

    Refuses LOUDLY on: blank `because`; an unknown/inactive seat; or no active managed_by
    edge out of it (nothing to detach)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — detaching a seat from its manager is a "
                         "deliberate act on the record"}
    row = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", (seat or "").strip())
    if row is None:
        return {"error": f"no such active seat: {seat!r}"}
    link = await actions.pool.fetchrow(
        "SELECT l.from_id, l.to_id, t.canonical AS manager FROM links l "
        "JOIN objects t ON t.id=l.to_id WHERE l.from_id=$1 AND l.type='managed_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", row["id"])
    if link is None:
        return {"error": f"{row['canonical']} has no active manager — nothing to detach"}
    now = datetime.now(UTC)
    await actions.invalidate_link(link["from_id"], link["to_id"], "managed_by", actor, now)
    await actions.assert_property(row["id"], "detached_because", because, actor, now, _CONF,
                                  evidence_class=_EC)
    return {"detached": row["canonical"], "was_managed_by": link["manager"], "because": because}


async def attach_seat(
    actions: Actions, worker: str, manager: str, *, evidence: str, actor: str,
) -> dict[str, Any]:
    """Create a managed_by edge — the mirror of detach_seat, and the other half of the
    toolkit hole named at thread fad0dc14. #99 built the way to CUT a management edge;
    nothing built the way to MAKE one except mint_seat's own birth-time create_link
    (mintseat.py:308, fires once, only at minting) and fold_seat's re-point (seats.py:1016,
    only for an existing edge). Every seat that predates mint_seat, was adopted, or lost
    its edge to a detach nobody re-pointed has had no path back except raw SQL — the
    graph's own defect report this house refuses to write around (raw SQL against the
    kernel is a missing verb, never a shortcut). Confirmed live: 30 active seats, 23 with
    no managed_by edge at all — the operator names Alfred managing eight; the graph, before
    this, could represent two.

    Refuses LOUDLY on: blank `evidence`; either seat unknown/inactive; `worker == manager`
    (a seat cannot manage itself); or an ALREADY-active managed_by edge out of `worker` —
    this is a CREATE, never a silent repoint, the same asymmetry detach_seat's own
    docstring draws (repointing onto a new manager is a different act than either half
    alone; detach first, then attach, if that's what's meant)."""
    evidence = (evidence or "").strip()
    if not evidence:
        return {"error": "evidence is required — attaching a seat to a manager is a "
                         "deliberate act on the record"}
    worker_row = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", (worker or "").strip())
    if worker_row is None:
        return {"error": f"no such active seat: {worker!r}"}
    manager_row = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Seat' "
        "AND status='active'", (manager or "").strip())
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
                         f"({existing['manager']}) — detach_seat first, then attach"}
    now = datetime.now(UTC)
    await actions.create_link(worker_row["id"], manager_row["id"], "managed_by", actor, now,
                              _CONF, evidence_class=_EC)
    await actions.assert_property(worker_row["id"], "attached_evidence", evidence, actor, now,
                                  _CONF, evidence_class=_EC)
    return {"attached": worker_row["canonical"], "now_managed_by": manager_row["canonical"],
            "evidence": evidence}
