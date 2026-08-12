"""Actor identity — the fleet made first-class ("a man and all his imaginary friends").

The persistent MCP server is ONE process the whole fleet writes through, so without this
every agent's writes collapse into the single `session` source — an undifferentiated mush.
This resolves each connecting agent into a distinct ACTOR and registers it in the graph:

  * project — from the agent's cwd (it always knows where it's working);
  * model — observed off the agent's OWN session record through the transcript store (the
    source-model provenance, authoritative from the harness, not the lying system prompt),
    anchored on its job dir;
  * session — the job/session id, the stable handle.

An `Agent` object (canonical `agent:<session>`) is minted with those, linked `works_in` its
project and `acts_for` the principal — so the graph literally contains its own operators, the
Palantir org chart with Claude instances as the analysts. Every write that agent then makes is
attributed to `agent:<session>` instead of `session`, which (a) makes provenance real — which
instance, which model, decided what — and (b) keeps the miner's ownership boundary intact
(an agent source is never `session-miner`, so the backfill miner never touches deliberate work).
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.ingest.harness import ModelReading
from src.ingest.sessions import (
    _job_id,
    locate_current_transcript,
    locate_transcript_by_cwd,
    model_of_transcript,
)
from src.orchestrator.offices import _DEFAULT_OFFICE_ROOT, is_bare_office_root
from src.orchestrator.swaps import classify_swap, swap_marker
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.agents")

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


@dataclass
class AgentIdentity:
    """Who an agent is. `agent_id` is the source string its writes are attributed to."""

    agent_id: str  # "agent:<session>" — the provenance source
    session: str
    project: str | None
    model: str | None
    cwd: str | None
    # HOW model was resolved — grades the source_model assertion (job_dir probe = observation,
    # cwd = a weaker guess, self_report = the agent's own word). None when model is unknown.
    model_method: str | None = None
    # divergence flag (ruling 17516660): the agent's SELF-REPORT of its model (if it passed one)
    # and whether it DISAGREES with the harness observation — a self-report that lies is a flag.
    model_declared: str | None = None
    model_divergent: bool = False
    # the distinct models across this session's transcript, first-seen order — the swap history
    # (>1 = a within-session demotion). Feeds the swap-detector (ruling f2ae6346).
    model_history: tuple[str, ...] = ()
    # the operator's own /model command is on this transcript's record — any within-session
    # swap was CHOSEN, not suffered (a seam, never a sin; complaint 2026-07-10).
    model_deliberate: bool = False
    # WHEN the anchored model observation was witnessed — the timestamp of the transcript
    # record that carried it (None when unanchored, or the record was unstamped). The seam
    # gate compares CLOCKS with this: the tail lags a /model until the next assistant turn,
    # so an observation not fresher than the stamp it disagrees with is a stale read, never
    # a seam (the TJMAX ping-pong, thread a3d49d91). source_model is stamped AT this moment
    # too — a ledger dated by the event, never by the bookkeeping.
    model_observed_at: datetime | None = None
    # False when identity fell back to a best-effort id (no session/job-id/transcript anchor). The
    # fallback is now DISTINCT per session — never the old shared `agent:unknown` sink — so distinct
    # actors can't merge; the flag lets the fleet digest surface an unresolved onboarding.
    resolved: bool = True
    # SET BY register_agent (the one out-param): "<prior> → <observed>" when this registration
    # crossed a succession seam — a fresh context inheriting an identity another model wrote under
    # (bug #51, a sibling project). None when no seam fired. mount() reads it to confess the seam.
    model_succession: str | None = None
    # SET BY register_agent: True when this mount RE-ATTACHED an identity carrying a winning
    # retired=true (bug #51 follow-up, a sibling project msg 69). The trigger already refuses to
    # reanimate the retired (resume-not-mint), but a plain mount from the same session UUID would
    # silently un-retire the name. register_agent now stamps the reanimation as a first-class
    # OBSERVED event and mount() confesses it — never a silent reanimation (membrane, rule #6).
    reanimated: bool = False
    # SET BY register_agent under the MINT ruling (be292762): this context was minted a NEW
    # lineage-linked id (agent:<base>-ii…) because it arrived across a detected seam or wore a
    # retired face. Holds the ANCESTOR's canonical; mount() confesses the minting to the heir.
    succeeded_from: str | None = None
    # COULD NOT READ A `.osiris` DECLARATION THAT EXISTS (Sekhmet's design, e3f4f159; Thoth
    # DM 2677 item 2) — the file was found but failed to parse/read, a DIFFERENT real thing
    # from "no declaration" (which stays the legitimate unpinned None, unchanged). Set only
    # when `_read_osiris_key`'s climb actually hit a broken file for the `project` key;
    # `project` above still falls back to the basename guess exactly as before. None/None
    # when nothing broke — the common case, never populated speculatively.
    project_pin_error: str | None = None
    # THE PATH DOES DOUBLE DUTY (task #128, wave 2, 2026-08-03): set alongside
    # `project_pin_error` for a broken file, OR ALONE (error=None) for a valid `.osiris`
    # that simply never declares `project` — the heinrich shape, correct TOML answering a
    # different question. `project_pin_banner` tells these apart by checking `project_pin_error`
    # first; a caller that only wants "is there a path to point at" can use this either way.
    project_pin_path: str | None = None
    # TRUE ONLY WHEN NO `.osiris` EXISTS ANYWHERE IN THE CLIMB AT ALL — the third leg of the
    # three-way split wave 2 needs (no pin · unparseable pin · parseable pin missing the
    # key). Never set for the bare seat-office root (ruling 577988ed's carve-out), when
    # there is no cwd to climb from at all, or when `cwd` itself doesn't exist (see
    # `project_pin_cwd_missing` below — a DIFFERENT, disjoint state) — those stay silent by
    # design, not "missing".
    project_pin_missing: bool = False
    # `cwd` ITSELF DOES NOT EXIST ON DISK (Thoth's catch, msg 3928, thread 3937) — set only
    # when `_read_osiris_key`'s leaf check fails before any climb even starts. Deliberately
    # DISJOINT from `project_pin_missing`: a deleted office and an unpinned-but-real office
    # are opposite dispositions (one wants the graph's stale belief reaped, the other wants
    # a pin written), and folding them into one flag is the exact 60bc15db this fixes.
    # `project` still falls back to a basename guess either way (unchanged) — this only
    # makes the WHY honest.
    project_pin_cwd_missing: bool = False
    # SET BY register_agent (task #144, rule 1 of de3dfc18 — "where this lineage's work
    # actually landed"): the majority in_repo target across this agent's OWN lineage,
    # reported HONESTLY, never used to overwrite `project` above. `write_attribution_agreement`
    # is one of "no-signal" (this lineage has never filed an in_repo edge anywhere) /
    # "confirms" (the majority target matches `project`) / "disagrees" (it doesn't) — never
    # a fourth, silent "picked its own answer" state; mount() confesses "disagrees", it never
    # acts on it. None/0/None when the DB check itself couldn't run (a degrade, never a block
    # — 577988ed governs the mount path this sits on).
    write_attribution_agreement: str | None = None
    write_attribution_top: str | None = None
    write_attribution_total: int = 0


# Roman generations for successor ids (a sibling's grammar: agent:a8c15486-ii). The alphabet
# is DELIBERATELY restricted to {i, v, x} — none of which are hex digits — so a full-UUID
# canonical
# like agent:2f81c6d5-…-0a7cd0e63f21 can never misparse its tail as a generation ('d' and 'c'
# are valid Roman AND valid hex; 'i'/'v'/'x' are Roman only). Caps the alphabet at 39 (xxxix); a
# lineage deeper than that gets a plain numeric suffix, still hex-collision-free.
_ROMAN_UNITS = [(10, "x"), (9, "ix"), (5, "v"), (4, "iv"), (1, "i")]


def _to_roman(n: int) -> str:
    if n > 39:
        return f"g{n}"
    out: list[str] = []
    for val, sym in _ROMAN_UNITS:
        while n >= val:
            out.append(sym)
            n -= val
    return "".join(out)


def _from_roman(s: str) -> int | None:
    if not s or set(s) - {"i", "v", "x"}:
        return None
    vals = {"i": 1, "v": 5, "x": 10}
    total = 0
    for a, b in zip(s, s[1:] + "\0", strict=False):
        v = vals[a]
        total += -v if vals.get(b, 0) > v else v
    return total if 0 < total and _to_roman(total) == s else None  # reject 'iiii' etc.


def _generation(canonical: str) -> tuple[str, int]:
    """(root, generation) — agent:x is generation 1; agent:x-ii is (agent:x, 2)."""
    root, sep, suffix = canonical.rpartition("-")
    if sep and root:
        g = _from_roman(suffix)
        if g is not None and g >= 2:
            return root, g
    return canonical, 1


def next_generation(canonical: str) -> str:
    root, gen = _generation(canonical)
    return f"{root}-{_to_roman(gen + 1)}"


# Full roman numerals for the human DISPLAY generation (Anna IV, Anna IX) — unlike the id
# suffix (restricted to i/v/x for hex-safety), a display label parses nothing, so it can use
# the whole numeral system.
_ROMAN_FULL = [(1000, "m"), (900, "cm"), (500, "d"), (400, "cd"), (100, "c"), (90, "xc"),
               (50, "l"), (40, "xl"), (10, "x"), (9, "ix"), (5, "v"), (4, "iv"), (1, "i")]


def _roman_display(n: int) -> str:
    out: list[str] = []
    for val, sym in _ROMAN_FULL:
        while n >= val:
            out.append(sym)
            n -= val
    return "".join(out).upper()


def normalize_model(model: str | None) -> str | None:
    """Canonical model id for MIND comparisons (the [1m] false-mint bug, 2026-07-09): the
    harness decorates display ids with a bracketed variant suffix (claude-opus-4-8[1m] is the
    1M-context tier of the SAME weights) while transcripts record the bare id. Same weights =
    same mind — a variant suffix must never read as a death. Every seam comparator and every
    stored model goes through this."""
    if not model:
        return model
    return model.split("[", 1)[0].strip()


# ── HOUSE · SEAT · HOLDER ────────────────────────────────────────────────────────────────
# THE OPERATOR'S RULING (2026-07-12): "the project name is the house (a-sibling), each
# function/job has a name (Ra), the holder dies and multiplies (ra I, ra II), but splitting to
# Ptah would break and confuse the lineage, and the fragmentation of agents was a bug in and of
# itself."
#
# The old model keyed a lineage to the ANCHOR (job_dir), so every new CONVERSATION minted a whole
# new bloodline — 1008 registered agents for ~20 real seats — and the name died with the
# conversation that held it. The next mind in the house woke nameless, reached for the family
# name, was refused as a stranger, and took a new one. That is how a-sibling's Ra became Ptah
# and a sibling project's Soundwave became "Soundwave VIII". The fragmentation WAS the bug.
#
# Two things were conflated, and only ONE of them follows the anchor:
#   · THE WRITER — agent:c7ef52a9-iii. A particular mind. Attribution stays exactly per-writer;
#     this is why the merge Ptah asked for was refused (4abaf52d) — his writes are his.
#   · THE SEAT — Ra, in the house a-sibling. A ROLE, held by successive writers.
# The seat sits ABOVE the writer, so nothing merges and nothing is falsified: Ptah's writes remain
# Ptah's, and he HOLDS the seat Ra — he is Ra V. Different mind, same job.


async def seat_holders(pool: asyncpg.Pool, house: str | None, seat: str) -> list[str]:
    """Every mind that has held this seat in this house, in the order they took it up. The
    generation IS the ordinal here — Ra I, Ra II — and it counts HOLDERS, not anchors."""
    return [r["canonical"] for r in await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Agent' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='project' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '') = COALESCE($2, '') "
        # a healed phantom never HELD the seat (thread 6c99800a: TJMAX read X when ~VI minds
        # ever acted). false_mint only — a RETIRED real holder still held it; filtering
        # retired would renumber history.
        "AND NOT EXISTS (SELECT 1 FROM current_assertions f WHERE f.object_id=o.id "
        "  AND f.name='false_mint' AND f.value #>> '{}' = 'true') "
        # ...and a VISITOR never held it either (Phase C, §4.3): a spawn wearing a handle is
        # the leak, not a holder — counting it would renumber every real generation after it.
        "AND NOT EXISTS (SELECT 1 FROM links sl WHERE sl.from_id=o.id "
        "  AND sl.type='spawned_by') "
        # a same-instant double-mint has no deterministic order on created_at alone —
        # Thoth's flag, DM 1301 — id tiebreaks it, matching compositions.py's own
        # "ORDER BY created_at, id" idiom elsewhere in this codebase.
        "ORDER BY o.created_at, o.id", seat, house)]


async def house_of(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The house an agent works in — its project. A seat belongs to a house."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='project' "
        "WHERE o.canonical=$1 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent_id)


async def correct_agent_house(
    actions: Actions, *, agent_id: str, project: str | None = None,
    seat_generation: int | None = None, actor: str,
) -> dict[str, Any]:
    """Heal an ALREADY-POLLUTED agent's own project/seat_generation stamps — the
    data-repair half of mount-guard #6 (Thoth's fused ask, DM 1301). A transient bad
    mount (the bare seat-office root, no .osiris pin) leaves a durable wrong `project`
    stamp on an Agent object — and, downstream through claim_name/mint_heir's now-
    fixed counting, a wrong `seat_generation` too — that commit cb47d02's code fix
    cannot itself heal: it only stops NEW pollution from taking root, deliberately.
    This is that healing act.

    UNLIKE correct_house: NOT self-scoped, on purpose. The target need not be the
    caller — Thoth's own case needed his PREDECESSOR's stamp corrected too, an
    ancestor who cannot act for itself. Accountability lives in `actor`, an explicit
    witness, not in a same-caller requirement. Append-only, same as everywhere in
    this kernel: asserts a new current value, never touches the superseded row.

    Refuses LOUDLY on: no correction named at all; an empty project string; a
    non-positive generation; an unknown or inactive Agent.

    PRIOR-ART SURFACED, NEVER REFUSED (obligation e4612853's sibling, ruling 38c71544's
    family): the receipt's own `prior_art`/`prior_art_flag` keys, when present, name a
    standing Decision that may already cover this agent's project/generation — the same
    search()-based guard record_decision runs on itself, generalized here. Cannot
    distinguish a deliberate correction from an uninformed overwrite; only ensures the
    write does not land silently unread."""
    if project is None and seat_generation is None:
        return {"error": "nothing to correct — pass project and/or seat_generation"}
    if project is not None and not project.strip():
        return {"error": "project cannot be corrected to an empty string"}
    if seat_generation is not None and seat_generation < 1:
        return {"error": "seat_generation must be a positive integer"}
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
    if row is None:
        return {"error": f"no such agent: {agent_id!r}"}
    if row["status"] != "active":
        return {"error": f"{agent_id} is {row['status']}, not active — nothing to correct"}
    now = datetime.now(UTC)
    was: dict[str, Any] = {}
    corrected: dict[str, Any] = {}
    from src.orchestrator.capture import property_prior_art

    prior_art_bits: dict[str, Any] = {}
    if project is not None:
        project = project.strip()
        was["project"] = await house_of(actions.pool, agent_id)
        await actions.assert_property(row["id"], "project", project, actor, now, _CONF,
                                      evidence_class=_EC)
        corrected["project"] = project
        prior_art_bits = await property_prior_art(
            actions.pool, subject_canonical=agent_id, field="project",
            new_value=project, actor=actor)
    if seat_generation is not None:
        was["seat_generation"] = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
            "AND a.name='seat_generation' ORDER BY a.confidence DESC, a.observed_at DESC "
            "LIMIT 1", row["id"])
        await actions.assert_property(row["id"], "seat_generation", str(seat_generation),
                                      actor, now, _CONF, evidence_class=_EC)
        corrected["seat_generation"] = seat_generation
        if not prior_art_bits:  # project's own hit (if any) already covers this agent;
            # never overwrite a real flag with a weaker/absent seat_generation-only search
            prior_art_bits = await property_prior_art(
                actions.pool, subject_canonical=agent_id, field="seat_generation",
                new_value=str(seat_generation), actor=actor)
    return {"agent_id": agent_id, "corrected": corrected, "was": was, **prior_art_bits}


async def retire_agent(
    actions: Actions, *, agent_id: str, actor: str, because: str,
    override_live: bool = False,
) -> dict[str, Any]:
    """Third-party retirement for an agent — the third-party-scoped complement to the
    self-scoped retire() (mcp_server.py derives the CALLER's own id, no target param at
    all). Task #74's own gap: msg 1713's reap needed exactly this — two genuinely-dead
    agents (Flip68Real's residue, e29d40ce/-ii) could only be retired via direct
    assert_property under the operator's own live permission grant, TWICE, because no
    sanctioned verb reached a THIRD PARTY.

    NOT SELF-SCOPED, and NOT MANAGER-GATED either — corrected 2026-08-02 (census
    a5e53ed8/3f97f9c7: an earlier docstring's own "manager-scoped" phrasing invited a
    reader to infer a check that has never existed here). Any mounted caller may name any
    active agent as the target, the SAME shape retire_seat/retire_project already carry —
    accountability lives in `actor`, an explicit witness, never an authority gate. If a
    manager-only restriction is ever wanted, it belongs here as a real check (mirroring
    charter_for's), not as prose a reader has to trust.

    Stamps `retired`/`retired_by`/`retired_because` (append-only assertions, the same
    free-form vocabulary this codebase already carries — `_PHANTOM_FOLD_SRC` etc. are
    not a strict enum) AND flips objects.status via Actions.set_status — the real
    compensating event, same pattern as retire_seat/retire_project, so a third-party
    retirement is auditable and never just a label (the STATUS GAP class already fixed
    twice elsewhere in this house).

    THE LIVENESS + SEAT/MOUNT GAP (thread 00b1c341, Khnum's census — scoped OUT of the
    authority build deliberately, it is not an authority defect): this used to do NONE of
    what retire()'s own self-retirement already does for the exact same act. Fixed by
    reusing two already-proven siblings rather than inventing a third mechanism:

    (1) LIVENESS — mounts.agent_liveness (already built for send()'s own listener receipt,
    "seen within 15 min") is the gate. retire_seat REFUSES outright on a live holder
    (protecting an occupant's ongoing work in ITS ROLE); vacate_holder instead TRUSTS ITS
    CALLER with no liveness check at all (its blast radius is one link + one property —
    small). retire_agent's blast radius is bigger (a terminal Agent status plus deleted
    mount rows), so blind trust under-protects a genuinely live third party — but this
    verb's own founding purpose (third-party cleanup of agents that can never call
    retire() on themselves) means a PERMANENT block would defeat it. The resolution is
    retire()'s OWN shape, reused rather than reinvented: refuse by default on a live
    target, naming the evidence, but accept `override_live=True` as a deliberate,
    on-the-record act — the same escape hatch retire()'s `acknowledge_leftovers` already
    is for its own preflight refusal.

    (2) SEAT/MOUNT RELEASE — unconditional on a successful retirement, live or not: this
    is the half of the bug with no defensible reason to stay broken (a corpse should never
    keep holding a seat). held_seat + vacate_holder (seats.py) release any active `holds`
    link the same way retire_seat's own vacate-then-retire discipline would, WITHOUT
    retiring the seat itself (the role may still get a legitimate new occupant — only
    retire_seat closes the role). mounts.release_mounts (thread b47b3814, retire()'s own
    call) drops the durable mount row so a retired agent never haunts the fleet chrome as
    a live mount, exactly as it already does for self-retirement.

    Refuses LOUDLY on: blank `because`; an unknown or already-non-active agent; a LIVE
    target unless `override_live=True`."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — retiring an agent is a deliberate act "
                         "on the record"}
    agent_id = (agent_id or "").strip()
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
    if row is None:
        return {"error": f"no such agent: {agent_id!r}"}
    if row["status"] != "active":
        return {"error": f"{agent_id} is already {row['status']} — nothing to retire"}

    from src.orchestrator import mounts

    liveness = await mounts.agent_liveness(actions.pool, agent_id)
    if liveness["live"] and not override_live:
        return {"error": f"{agent_id} is LIVE right now (last_seen {liveness['last_seen']}) "
                         "— retire_agent refuses to pronounce a live mind dead by default; "
                         "pass override_live=True to retire it anyway, a deliberate act on "
                         "the record (mirroring retire()'s own acknowledge_leftovers escape "
                         "hatch)", "liveness": liveness}

    now = datetime.now(UTC)
    await actions.assert_property(row["id"], "retired", "true", actor, now, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(row["id"], "retired_by", actor, actor, now, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(row["id"], "retired_because", because, actor, now, _CONF,
                                  evidence_class=_EC)
    await actions.set_status(row["id"], "retired", because, actor)

    from src.orchestrator.seats import held_seat, vacate_holder

    out: dict[str, Any] = {"retired": agent_id, "because": because,
                           "was_live": liveness["live"]}
    bound = await held_seat(actions.pool, agent_id)
    if bound is not None:
        vac = await vacate_holder(actions, seat_id=bound["seat_id"], actor=actor,
                                  because=f"holder retired: {because}")
        if vac.get("vacated"):
            out["seat_vacated"] = vac["vacated"]
    out["mount_rows_released"] = await mounts.release_mounts(actions.pool, agent_id)
    return out


def seat_label(canonical: str, handle: str | None, generation: int | None = None) -> str | None:
    """The human display for an agent: 'Ra V' — the SEAT plus which holder of it this mind is.

    `generation` is the ordinal among the seat's HOLDERS (stamped at claim time). It falls back to
    the anchor's roman suffix only for agents claimed before the house/seat ruling — under which a
    successor in a NEW conversation would restart at I and collide with its own ancestors."""
    if not handle:
        return None
    gen = generation if generation is not None else _generation(canonical)[1]
    # the FIRST life wears its numeral too — 'Alfred I', never bare (operator ruling,
    # 2026-07-16: 'so there is continuity even at the first')
    return f"{handle} {_roman_display(gen)}"


# a trailing roman numeral is a SEAT, not a name ("Soundwave VIII" is what the substrate calls
# Soundwave's 8th mind). Claiming it as a handle forks the lineage — see claim_name.
_SEAT_SUFFIX = re.compile(r"[\s_-]+(?:[IVXLC]+)\s*$", re.IGNORECASE)

async def claim_name(actions: Actions, agent_id: str, name: str, *, source: str) -> dict[str, Any]:
    """An agent names itself (ruling 1e02e069): the intelligence picks a meaningful name, the
    substrate enforces uniqueness. Refuses a name held by a DIFFERENT lineage (permanent
    exhaustion — a name belongs to one lineage forever; a successor inherits it automatically,
    a stranger cannot take it). Global namespace → unambiguous addressing. Stamps `handle` on
    the agent's Agent object (SELF_DECLARED).

    A HANDLE IS A NAME. THE GENERATION IS A NUMERAL THE SYSTEM ASSIGNS (operator, 2026-07-12:
    "soundwave and Ra claim to belong to a different lineage, did that break recently?" — it
    had, sixteen hours earlier). The uniqueness guard below was defeated by a SUFFIX: a fresh
    a sibling session read its own SEAT LABEL — "Soundwave VIII" — and claimed that STRING as
    its name. "Soundwave VIII" != "Soundwave", so the check waved it through, minting a new
    handle and therefore a NEW LINEAGE ROOT, orphaning Soundwave's eight real generations. The
    agent was not confused; it was misfiled, and then it correctly reported belonging to a
    different lineage. So: strip the numeral before judging the name, and refuse the claim —
    a seat label is something the substrate SAYS about you, never something you may call
    yourself."""
    name = (name or "").strip()
    if not name or name.lower().startswith("agent:") or len(name) > 40:
        return {"error": "pick a short human name (not an id)"}
    # A VISITOR MAY NOT CLAIM A SEAT (Phase C, ruling 5cef856b / spec §4.3 — alfred's dead
    # builder-orphans ce348dc5/42bf712d were spawns that became project PEERS): a sub-agent
    # works in its parent's name and returns its result; the seat, its mail, and its
    # succession belong to the parent.
    spawner = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects o ON o.id=l.from_id "
        "JOIN objects p ON p.id=l.to_id "
        "WHERE o.canonical=$1 AND l.type='spawned_by' LIMIT 1", agent_id)
    if spawner:
        return {"error": f"a VISITOR may not claim a seat: {agent_id} was spawned_by "
                         f"{spawner} — a sub-agent works in its parent's name; the seat, "
                         "its mail, and its succession belong to the parent."}
    bare = _SEAT_SUFFIX.sub("", name).strip()
    if bare and bare.lower() != name.lower():
        return {"error": f"'{name}' is a SEAT LABEL, not a name — the numeral is the generation, "
                         f"and the substrate assigns it. Claim '{bare}' if that lineage is "
                         "yours to continue; otherwise pick a name of your own."}
    # GLOBAL FIRST, HOUSE-SCOPED ONLY WHEN GENUINELY NEW (thread cb374585): a real,
    # unambiguous seat for this handle can be VACANT (no holder to disagree with a stale
    # house guess) — find_seat's own (house, handle) lookup silently misses it whenever the
    # caller's own computed house doesn't match what's actually stored, and used to mint a
    # SECOND seat instead (the Vajra twin, seat:1d3cf119, born this exact way while the real
    # seat:191f1a1e — managed_by Alfred — sat untouched). seats_by_handle answers the
    # question find_seat can't: does ANY active seat already carry this name, regardless of
    # house? Zero → mint fresh, house-scoped is correct (nothing to conflict with). One →
    # THAT seat, always, whatever its own stored house says. Two or more → an ambiguity
    # (a twin) this claim refuses rather than silently arbitrates; fold_seat resolves it
    # deliberately, on its own turn, never as a side effect of an unrelated claim.
    # Resolved HERE, early, because the seat's own id is also THE COUNTING HOUSE below —
    # not a separate concern to revisit after the generation math runs.
    from src.orchestrator.seats import bind_holder, derive_house, ensure_seat, seats_by_handle
    existing = await seats_by_handle(actions.pool, name)
    if len(existing) > 1:
        return {"error": f"'{name}' names {len(existing)} active seats — an ambiguity this "
                         f"claim will not silently arbitrate: {', '.join(existing)}. A "
                         "deliberate fold_seat resolves a twin; claim_name never guesses."}
    seat_id: str | None = existing[0] if existing else None
    # A SEAT BELONGS TO A HOUSE, AND AN HEIR INHERITS IT (operator's ruling, 2026-07-12). The old
    # guard keyed a name to a LINEAGE ROOT — the anchor — so the moment a conversation ended, its
    # name died with it: the next mind in the same house reached for the family name, was refused
    # as a "stranger", and took a new one. That is how Ra became Ptah. Now the question is not
    # "were you minted under the same job_dir" but "do you work in the same house".
    house = await house_of(actions.pool, agent_id)
    holders = await seat_holders(actions.pool, house, name)
    # THE COUNTING HOUSE IS THE SEAT'S, NOT THE CALLER'S (Thoth's fused ask, DM 1301, live
    # case: a transient wrong-house mount — a container-root cwd with no seat pin —
    # miscounted a 58-generation reign as generation 2). When a real seat already exists,
    # its own derive_house (the managed_by-chain-derived, lineage-authoritative house — same
    # discipline as held_seat/manager_of_seat) is the counting authority for GENERATION MATH
    # ONLY — kept deliberately separate from `holders` above, which the elsewhere-check just
    # below still needs scoped by the CALLER's own house: that guard's whole job is "does my
    # OWN house have zero history with this name", and answering it with the seat's house
    # instead would let an outsider from a genuinely different house walk straight past it
    # (a real regression, caught by test_the_house_the_seat_and_the_holders — an outsider in
    # 'sibling-one' must still be refused a seat whose true, derived house is 'sibling-two').
    # A genuinely EMPTY derived house (a seat minted before any project was known) is treated
    # like "no seat yet" — trusting an empty stamp over the caller's own real one regressed
    # mint_heir's sibling case (test_the_whisper_honors_a_bound_seat); same discipline here.
    _derived = await derive_house(actions.pool, seat_id) if seat_id else None
    counting_house = _derived if _derived else house
    counting_holders = (holders if counting_house == house
                        else await seat_holders(actions.pool, counting_house, name))
    elsewhere = await actions.pool.fetchrow(
        "SELECT o.canonical FROM objects o JOIN current_assertions h ON h.object_id=o.id "
        "AND h.name='handle' WHERE o.type='Agent' AND lower(h.value #>> '{}') = lower($1) "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='project' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '') "
        "  <> COALESCE($2, '') LIMIT 1",
        name, house)
    if elsewhere is not None and not holders:
        return {"error": f"'{name}' is a seat in another house ({elsewhere['canonical']}) — a name "
                         "belongs to one house; pick a name for your own."}
    # a seat a LIVE mind is already sitting in is not vacant: two minds in one house do two jobs.
    # UNCONDITIONAL now (thread cb374585): gating this behind `holders` — an AGENT-history,
    # house-scoped count — meant a caller whose own computed house didn't match the seat's
    # own stored house skipped the seat-world check entirely, exactly the Vajra shape (a
    # fresh session's CWD-derived house disagreeing with the seat Alfred actually minted).
    sitting = await resolve_seat(actions, name)
    if sitting["live"] and sitting["agent"] != agent_id:
        return {"error": f"'{name}' is currently held by {sitting['agent']}, who is LIVE — a seat "
                         "is a job, and two minds in one house do two jobs. Take another seat, or "
                         "wait for this one to be vacated."}
    a = await actions.create_or_find_object("Agent", agent_id, source)
    now = datetime.now(UTC)
    await actions.assert_property(a, "handle", name, source, now, _CONF, evidence_class=_EC)
    # the generation counts HOLDERS of this seat in ITS OWN house — not anchors, not
    # conversations, and not the caller's possibly-wrong house (see counting_house above)
    gen = ((counting_holders.index(agent_id) + 1) if agent_id in counting_holders
          else len(counting_holders) + 1)
    await actions.assert_property(a, "seat_generation", str(gen), source, now, _CONF,
                                  evidence_class=_EC)
    # THE SUCCESSION EDGE (Ra V, a-sibling, msg 374): "the graph finally gets the parent edge
    # it's been missing". Before this, successor seats carried NO edge to their ancestor, so a
    # lineage was not WALKABLE from the record — which is exactly why Ra could not tell his
    # CONTEMPORARY from his own ghost, and asked me to merge them. A seat's history must be
    # traversable, or the next mind re-derives it from the disk the way he had to.
    # THE PREDECESSOR IS THE HOLDER BEFORE ME — not "the last holder unless it happens to be me".
    # That older reading silently skipped the edge for the one case that needs it most: an heir
    # minted by mint_heir ALREADY carries the inherited handle, so it is already in
    # `counting_holders`, and as the newest it IS counting_holders[-1] — which resolved `prior`
    # to None and minted nothing. A mind that inherited its seat could not claim its own
    # ancestry. (The ghosts, 53729dd6.)
    if agent_id in counting_holders:
        i = counting_holders.index(agent_id)
        prior = counting_holders[i - 1] if i > 0 else None
    else:
        prior = counting_holders[-1] if counting_holders else None
    if prior:
        await actions.create_link(
            a, await actions.create_or_find_object("Agent", prior, source),
            "succeeds_seat", source, now, _CONF, evidence_class=_EC)
    # THE SEAT-WORLD ON-RAMP (5cef856b — the designed-but-unshipped half, caught in the
    # bytebye pilot: 'called at claim_name and daemon spawn' had only the daemon wired). A
    # successful claim mints/finds the Seat OBJECT and binds the claimer as its holder — a
    # claim is the assertion world's own deliberate binding act, and every guard above
    # (visitor, live-sitter, other-house) already ran. Legacy seats enter the Seat world
    # the moment they are next claimed; from there succession, mail, resolution, and
    # resume all ride the durable binding. `seat_id` was already resolved above (it doubles
    # as the counting house's own key) — only the genuinely-new-handle case has minting left
    # to do here.
    if seat_id is None:
        seat_world = await ensure_seat(actions, house=counting_house, handle=name, source=source)
        if not seat_world.get("error"):
            seat_id = seat_world["seat_id"]
    if seat_id:
        await bind_holder(actions, seat_id=seat_id, agent_id=agent_id, source=source)
    return {"claimed": name, "seat": seat_label(agent_id, name, gen), "agent": agent_id,
            "house": counting_house, "generation": gen, "inherited_from": prior,
            **({"seat_id": seat_id} if seat_id else {})}


async def resolve_seat(actions: Actions, name: str) -> dict[str, Any]:
    """A human name → WHICH SEAT OF THAT LINEAGE IS ACTUALLY ALIVE, and the truth about it.

    THE GRAVE-DELIVERY BUG (two seats on two different projects, independently, within one
    hour, 2026-07-12). The old resolver ordered by `m.last_seen DESC NULLS LAST` and filtered
    NOTHING — so a seat dead for three days, carrying a stale mount row, outranked a live successor
    that had no mount row at all. send(to_agent='Soundwave') delivered into a grave, returned
    sent=360, and the only signal was a boolean the caller had to notice himself. Atlas II's entire
    port report died in a corpse's inbox the same way — and HIS receipt said live=true, because
    liveness was read off one seat while delivery went to another.

    A RECEIPT MUST DESCRIBE THE SEAT THAT ACTUALLY RECEIVED. This is not a cosmetic misroute: every
    mount banner tells the fleet "DM me as send(to_agent='Anubis')", so the DOCUMENTED path was the
    broken one — and a dead seat accepts mail exactly like a live one, which makes the loss silent.
    Lineages that turn over fastest resolved wrongest, so the blast radius grew with the fleet's
    health. Anubis X had already told the fleet to stop using names at all.

    Now: retired and false-mint seats are never candidates (reaching a grave takes an explicit
    agent id — an act of intent, not a banner a tired mind followed); a LIVE seat always wins; and
    among equals the LATEST GENERATION wins, because an heir outranks its ancestor. The whole
    picture is returned so the caller can warn LOUDLY instead of hiding it in a field.

    THE BINDING OUTRANKS THE INFERENCE (Phase B1, ruling 5cef856b): when the Seat-OBJECT
    world has an authoritative answer — a unique living Seat carrying this handle, with an
    active holder — that holder wins outright, before any liveness ranking runs. The
    assertion path below ranks GUESSES by heat, and a hotter mount row on a stale
    generation is exactly the grave-delivery shape; a declared binding is not a guess.
    The assertion path remains, whole, as the fallback for every un-seated lineage.
    """
    from src.orchestrator.seats import binding_of_handle
    bound = await binding_of_handle(actions.pool, name)
    if bound is not None:
        pulse = await actions.pool.fetchval(
            "SELECT max(last_seen) FROM agent_mounts WHERE agent_id=$1", bound["holder"])
        live = bool(pulse and (datetime.now(UTC) - pulse).total_seconds() < 900)
        out_b: dict[str, Any] = {
            "name": name, "agent": bound["holder"], "live": live,
            "candidates": [bound["holder"]], "seat_id": bound["seat_id"],
        }
        if not live:
            out_b["warning"] = (
                f"NO LIVE SESSION holds '{name}' — {bound['holder']} is bound to "
                f"{bound['seat_id']} but is NOT listening. This message may never be read.")
        return out_b
    rows = await actions.pool.fetch(
        "SELECT o.canonical, m.last_seen, "
        " (m.last_seen > now() - interval '15 minutes') AS live, "
        " COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='seat_generation' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '0') AS gen "
        "FROM objects o "
        "LEFT JOIN agent_mounts m ON m.agent_id=o.canonical "
        "WHERE o.type='Agent' "
        # the WINNING handle: one mind, one seat. A re-seated agent keeps its old claim in the
        # record at a lower grade, and it must not answer to the name it no longer holds.
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions r WHERE r.object_id=o.id "
        "  AND r.name IN ('retired','false_mint') AND r.value #>> '{}' = 'true') "
        # a VISITOR never answers to a name (Phase C, §4.3): a spawn wearing a handle is a
        # leak, and resolving mail into it buries the message in a sidechain nobody resumes
        "AND NOT EXISTS (SELECT 1 FROM links sl WHERE sl.from_id=o.id "
        "  AND sl.type='spawned_by') "
        "ORDER BY m.last_seen DESC NULLS LAST", name)
    if not rows:
        return {"name": name, "agent": None, "live": False, "candidates": []}
    # a LIVE holder always wins; among the dead, the LATEST HOLDER of the seat (not the highest
    # anchor numeral, which says nothing once a seat outlives its first conversation)
    best = max(rows, key=lambda r: (bool(r["live"]), int(r["gen"] or 0)))
    out: dict[str, Any] = {
        "name": name, "agent": best["canonical"], "live": bool(best["live"]),
        "candidates": [r["canonical"] for r in rows],
    }
    if not best["live"]:
        out["warning"] = (
            f"NO LIVE SEAT holds '{name}' — {best['canonical']} is the newest seat of that "
            "lineage and it is NOT listening. This message may never be read.")
    return out


async def resolve_handle(actions: Actions, name: str) -> str | None:
    """A human name → the LIVE seat of that lineage.

    See resolve_seat — that word does a great deal of work.

    ONE MORE DISTINCTION resolve_seat's own bare `agent` field can't make (task #142 punch-
    list item 3, Thoth's dispatch): when `name` is a unique Seat whose only holder(s) are all
    ineligible (retired/false_mint/visitor), `binding_of_handle` returns None and resolve_seat
    falls to its un-seated-lineage fallback — which, by its own WHERE clause, ALSO excludes
    that ineligible holder, so it can resolve to some OTHER, older, unmarked generation of the
    same lineage instead: the exact grave-delivery shape rulings 1a64ae9a/aee67e6d named,
    just reached through this wrapper instead of send(). Both of `resolve_handle`'s own
    callers (establish_office, rebind_seat) already have a correct "resolve to nothing → use
    the Seat object directly" fallback for exactly this situation — they only need
    `resolve_handle` to actually SAY nothing rather than hand them a wrong-but-real-looking
    agent id. `seat_holder_ineligible` returning non-None IS that distinction: return None
    instead of trusting the fallback's guess."""
    from src.orchestrator.seats import seat_holder_ineligible
    if await seat_holder_ineligible(actions.pool, name) is not None:
        return None
    return (await resolve_seat(actions, name))["agent"]  # type: ignore[no-any-return]


async def agent_seat(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The display seat for an ALREADY-RESOLVED agent id — 'Ra V', or None when this id is
    anonymous (no claimed handle). Reads the WINNING handle + seat_generation off
    current_assertions, the identical predicate seat_bearings/claim_name use — so a caller
    checking "does this id hold a seat" (dd47c1da: send(to_agent=...) must HARD-FAIL on an
    unclaimed target, require_seat=true) sees the same truth the roster and the claim guard
    see. Unlike resolve_seat, this takes an id already in hand — it answers "who IS this",
    never "which seat of a name is live" (that question is resolve_handle's)."""
    row = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='seat_generation' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS gen "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Agent'", agent_id)
    if row is None or not row["handle"]:
        return None
    return seat_label(agent_id, row["handle"], int(row["gen"]) if row["gen"] else None)


@dataclass(frozen=True)
class OsirisKeyRead:
    """One key's lookup result in a `.osiris` file (Sekhmet's design, e3f4f159; widened for
    task #128 wave 2, 2026-08-03; widened again for the missing-cwd gap, Thoth's catch msg
    3928/thread 3937): FOUR tell-apart-able states, not three — collapsing any pair of them
    hid a real bug or a real gap behind a shared `None`.

    THE QUERIED DIRECTORY DOES NOT EXIST AT ALL — `cwd_missing=True`, `value=None,
    error=None, path=None`. Checked BEFORE any climb: a deleted office (flip68real,
    resumelanecheck — real, now-retired Seats whose directories are gone) must never
    silently inherit an ANCESTOR's declaration. Without this state, a query against a
    deleted `~/.osiris/seats/flip68real` climbed straight past it to the enclosing
    seats-container's own pin and reported that as flip68real's OWN state — collapsing
    "this office is gone" into "this office exists, pin unset," two conditions with
    OPPOSITE dispositions (one wants a pin written, the other wants the graph's belief
    reaped). This is the single leaf check, never re-applied per ancestor: once `cwd`
    itself is confirmed real, every entry in `cwd.parents` is necessarily real too (a
    filesystem cannot have an existing child under a nonexistent parent).

    NO `.osiris` ANYWHERE IN THE CLIMB — `cwd` itself is real, genuinely nothing declared,
    ever: `value=None, error=None, path=None, cwd_missing=False`. The plain "never pinned"
    case — tell apart from the missing-directory state above ONLY by `cwd_missing`; every
    other field looks identical, which is exactly why collapsing them was invisible for as
    long as it was.

    FOUND, VALID, BUT NEVER SETS THIS KEY — e.g. REPOS/heinrich: a valid TOML file that
    declares `model` and never `project`. NOT a couldn't-read (it parses fine) and NOT the
    same as no file at all (a reader who only checks `value is None` would conflate "never
    touched" with "deliberately configured, just not for this"): `value=None, error=None,
    path=<the file>`. This is the shape no "has a pin" check will ever catch — the file
    LOOKS like protection and isn't.

    COULD NOT READ — the file exists but `tomllib.loads`/`Path.read_text` raised
    (TOMLDecodeError/OSError/ValueError) — someone WROTE a pin and it doesn't work:
    `value=None`, `error`=the exception's own text, `path`=the exact `.osiris` file that
    failed. Tell apart from the previous state by `error` being set.

    A caller that only wants the plain fallback-to-basename value uses `.value` and never
    needs to know which of the four produced it; `project_pin_banner` is where the
    distinction becomes four different messages."""

    value: str | None
    error: str | None = None
    path: str | None = None
    cwd_missing: bool = False


def _read_osiris_key(cwd: str | None, key: str) -> OsirisKeyRead:
    """One key from the repo's `.osiris` file (TOML), walking up to the repo root. See
    `OsirisKeyRead` for the four-way missing-directory / no-file / found-but-unset /
    could-not-read distinction this must keep tell-apart-able (Sekhmet's design, e3f4f159;
    widened msg 3928).

    `cwd` ITSELF MUST EXIST BEFORE ANY CLIMB BEGINS (Thoth's catch, msg 3928): a query
    against a directory that was never created or has since been deleted must never
    silently return an ANCESTOR's declaration as if it belonged to the queried path — the
    climb answers "what does an existing address near here declare", and a nonexistent
    address has no "near here" that means anything. Checked ONCE, on `cwd` alone: every
    entry in `cwd.parents` is guaranteed to exist once `cwd` itself does (a filesystem
    cannot have a real child under a nonexistent parent), so no per-level re-check is
    needed once this leaf check passes.

    THE CLIMB DOES NOT STOP AT A WORKTREE OR SUBMODULE BOUNDARY (task #128, root-cause
    finding, 2026-08-05): a git worktree's own `.git` is a FILE (a gitlink to
    `<root>/.git/worktrees/<name>`), not a directory — `Path.exists()` is true for it
    exactly as for a real repo root, so the old `.exists()` check stopped the climb one
    layer too early, before it ever reached the true root's pin. Every seat's own code
    checkout (`.claude/worktrees/<seat>`, the fleet's OWN mandated working location, ruling
    bcfdfcc1) is exactly this shape and carries no `.osiris` of its own — so this climb-stop
    silently fell back to the worktree's basename (the seat's own name) instead of the
    governed project every single time a seated agent mounted from its own code checkout.
    `.is_dir()` stops only at a REAL repo root; a gitlink file is transparent to the climb,
    so it continues up to the enclosing repo's own pin.

    THE CLIMB DOES NOT STOP AT A FILE THAT EXISTS BUT DOESN'T DECLARE THIS KEY, EITHER
    (live regression, caught and reverted the same minute it happened, ruling 719ed5b1's
    schema rollout): a worktree pin newly declaring `seat`/`house`/`kind` sits BELOW its
    repo root's own pin declaring `project`/`model` — a LAYERED declaration this function
    was never exercised against before. The old code treated `f.is_file()` as a hard stop
    regardless of whether the file answered the key being asked, so writing house/seat/kind
    into a worktree that relied on climbing to its root for `project` silently broke
    `project` resolution the instant the file existed — `read_project_label('.../worktrees/
    imhotep')` went from `"osiris"` to `None` mid-session, for every live agent in that
    worktree. Now: a file found without the key is REMEMBERED (the nearest one, for the
    heinrich-shape diagnostic) but the climb CONTINUES past it — only a real VALUE, a
    parse/read ERROR, or reaching the true repo root without ever finding the key
    terminates it. A single-level pin that simply never sets a key (REPOS/heinrich) still
    reports that file's own path when nothing further up sets it either — unchanged for
    every caller that never stacks declarations across levels; only the layered case
    behaves differently now, and correctly."""
    if not cwd:
        return OsirisKeyRead(value=None)
    p = Path(cwd)
    if not p.is_dir():
        return OsirisKeyRead(value=None, cwd_missing=True)
    import tomllib
    found_but_unset: str | None = None  # nearest file that exists but never sets `key`
    for d in (p, *p.parents):
        f = d / ".osiris"
        try:
            if f.is_file():
                value = tomllib.loads(f.read_text()).get(key)
                if value:
                    return OsirisKeyRead(value=str(value).strip())
                if found_but_unset is None:  # keep the NEAREST — an ancestor may still set it
                    found_but_unset = str(f)
        except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
            return OsirisKeyRead(value=None, error=f"{type(exc).__name__}: {exc}",
                                 path=str(f))
        if (d / ".git").is_dir():  # the TRUE repo root — a worktree/submodule gitlink
            break                  # (a FILE) never stops the climb, only a real root does
    return OsirisKeyRead(value=None, path=found_but_unset)


def read_project_label(cwd: str | None) -> str | None:
    """A project's DECLARED name, from a `.osiris` file (TOML: project = "..."), walking up to
    the repo root. Decouples the project identity from the FOLDER name (the operator may rename
    the dir; the label is a stable property of the repo — ruling 1e02e069). None → fall back to
    the cwd basename (silently, on EITHER no-declaration or could-not-read — callers that need
    to tell those apart and confess a broken pin use `read_project_pin`, e.g. resolve_identity)."""
    return _read_osiris_key(cwd, "project").value


def read_project_model(cwd: str | None) -> str | None:
    """A repo's DECLARED model intent (TOML: model = "claude-haiku-4-5" in `.osiris`) — the
    operator's PER-PROJECT standing choice. A fleet of onboarded repos does not all run the
    box default: a deliberately-haiku repo confessing 'not fable' every turn framed the
    operator's own choice as a sin (complaint, 2026-07-10). None → the box-wide default."""
    return _read_osiris_key(cwd, "model").value


def read_house_label(cwd: str | None) -> str | None:
    """A tree's DECLARED house (TOML: house = "..." in `.osiris`) — the governing org anchor,
    ruling 719ed5b1's pin-schema build. Distinct from `project`: a seat's own office pins
    house == project (577988ed), but a code checkout governed by that seat can legitimately
    declare a different `project` (its own repo's label) while `house` still names who governs
    it — the split this key exists to make offline-readable instead of graph-only. None → no
    house declared here (never a basename guess; unlike `project`, there is no folder-name
    fallback that means anything for an org anchor)."""
    return _read_osiris_key(cwd, "house").value


def read_seat_handle(cwd: str | None) -> str | None:
    """The HANDLE of the seat this tree belongs to (TOML: seat = "..." in `.osiris`), ruling
    719ed5b1: "the .osiris pin has no seat field — the declaration of record cannot declare
    who lives there." A handle, not a seat:uuid — matches how the fleet already addresses
    seats everywhere (mail, fleet(), roster()); a rename drifts this the same way it drifts
    any handle-keyed reference, detectable and re-syncable by the migration verb, never a
    silent corruption. None → no seat declared (a bare code checkout nobody's office is)."""
    return _read_osiris_key(cwd, "seat").value


def read_tree_kind(cwd: str | None) -> str | None:
    """What KIND of tree this is (TOML: kind = "..." in `.osiris`) — one of office | worktree |
    repo | container, ruling 719ed5b1: "nothing distinguishes an OFFICE from a WORKTREE from a
    plain REPO from a CONTAINER, so every consumer re-guesses from path shape." `container` is
    the data-level replacement for the hardcoded path-equality carve-outs
    (`offices.is_bare_office_root` et al.) — read here, not yet consumed by them (that fold-in
    is separate, deliberate follow-up work, not this key's own landing). None → undeclared;
    callers keep whatever path-shape guess they used before this key existed."""
    return _read_osiris_key(cwd, "kind").value


def _write_model_pin_sync(office: Path, model: str) -> bool:
    """THE SYNCHRONOUS HALF (task #146, operator's own complaint: "my /model confuses
    everything, it should be authoritative and automatically handle updating .osiris").
    Writes to the SEAT'S OWN OFFICE specifically — never `identity.cwd` as given, which may
    be a code worktree or a repo root several directories up the climb (#128) that other
    seats' sessions also read: a pin write must never become a cross-seat side effect.
    `_scaffold_office`'s own convention (office/.osiris carrying project AND model together)
    is preserved, not forked into a second file — reads `project` back out if a pin already
    exists so this never drops it, then rewrites both keys. Idempotent: returns False
    (nothing written) when the file already reads exactly this model, so an unchanged
    /model choice does not churn the disk on every subsequent mount.

    Never called with anything but a harness-OBSERVED model string (`SwapVerdict.to_model`,
    always a `deliberate` — a WITNESSED /model transition, ruling f2ae6346's own gate) — a
    bare alias a human might type (ptah's "sonnet") never reaches this function, only what
    the harness itself reported running. Still refuses a value that cannot be a real model
    id (empty, or containing a quote/newline that would corrupt the TOML) as a defensive
    floor, never a validated allowlist — model ids change over time and this file has no
    business hard-coding them."""
    if not model or '"' in model or "\n" in model:
        return False
    import tomllib
    pin = office / ".osiris"
    project: str | None = None
    if pin.is_file():
        try:
            existing = tomllib.loads(pin.read_text())
        except (OSError, tomllib.TOMLDecodeError, ValueError):
            existing = {}
        if existing.get("model") == model:
            return False  # already correct — no churn
        project = existing.get("project")
    office.mkdir(parents=True, exist_ok=True)
    lines = ([f'project = "{project}"'] if project else []) + [f'model = "{model}"']
    pin.write_text("\n".join(lines) + "\n")
    return True


async def write_model_pin(seat_handle: str, model: str) -> bool:
    """THE WRITE SIDE (task #146): update the seat's own `.osiris` pin so it becomes a CACHE
    of the operator's last /model decision rather than a competing, silently-stale claim —
    the gap named directly: nothing in this codebase ever wrote the model pin before this;
    `.osiris`'s `model =` key was read at launch and hand-edited only. Runs on a thread —
    `_stamp_alive`'s own convention for filesystem I/O inside an async miner/handler."""
    import asyncio
    office = _DEFAULT_OFFICE_ROOT / seat_handle.lower()
    return await asyncio.to_thread(_write_model_pin_sync, office, model)


def read_project_pin(cwd: str | None) -> OsirisKeyRead:
    """The FULL `project`-key read behind `read_project_label` — value plus, when a
    `.osiris` file exists but failed to parse/read, the path and error a banner can act on
    (Sekhmet's design, e3f4f159; Thoth DM 2677 item 2). `resolve_identity` uses this one,
    because it's the seam that carries the couldn't-read signal into `AgentIdentity` for
    mount()/orient() to confess. Everything else that only wants the plain fallback-to-
    basename value keeps using `read_project_label` — unchanged, still a bare `str | None`."""
    return _read_osiris_key(cwd, "project")


def project_pin_banner(ident: AgentIdentity) -> str | None:
    """WAVE 2 (task #128, operator's word via Thoth, 2026-08-03; wave 1 was DM 2677's
    couldn't-read-only banner, gated "DO NOT ARM THE REFUSAL" until the 29-name
    UNPINNED-LUCKY survey — b3a1f987 — was in hand). WARN, never refuse: every directory
    that falls back to a basename guess is confessed, but mount() still succeeds. The same
    SHAPE as the model-swap confession (`swaps.swap_banner`): loud, second-person, names
    exactly what's wrong, where, and the ONE fix that clears it — never a bare "cannot
    resolve". Silent for the bare seat-office root (577988ed's carve-out, unchanged) and
    when a real pin was found and used (the common, healthy case).

    FOUR DISTINCT MESSAGES now, not three — each a distinct repair (b3a1f987's own finding:
    a check keyed on "has a pin" would never catch the middle two; msg 3928 found the
    canonical reader itself was blind to a fourth):
      CWD DOES NOT EXIST (msg 3928) — the address itself is a ghost; no pin write repairs
        this, only reaping the graph's stale belief about it does.
      NO .osiris ANYWHERE — write one.
      FOUND, VALID, NEVER DECLARES `project` (the heinrich shape: a deliberately-written
        file answering a different question) — add the missing key, keep the rest.
      COULD NOT BE READ (broken TOML) — fix the syntax error named in the message."""
    if ident.project_pin_cwd_missing:
        return (
            f"⚠ {ident.cwd} DOES NOT EXIST ON DISK — nothing can be read here, and no "
            f"ancestor's declaration should be borrowed for it either (msg 3928: the old "
            f"climb silently did exactly that). Your project fell back to a BASENAME GUESS "
            f"({ident.project!r}) for an address that is not real. If this office was "
            "retired, the fix is reaping its stale graph beliefs, never writing a pin here."
        )
    if ident.project_pin_error:
        return (
            f"⚠ .osiris AT {ident.project_pin_path} COULD NOT BE READ "
            f"({ident.project_pin_error}) — this directory HAS a project pin, but it's "
            f"broken, so your project fell back to a BASENAME GUESS ({ident.project!r}) "
            "instead of reading it. Fix the file's TOML syntax (the error above names "
            "exactly what's wrong), then add `project = \"...\"` if it isn't there yet."
        )
    if ident.project_pin_path:
        return (
            f"⚠ .osiris AT {ident.project_pin_path} NEVER DECLARES `project` — the file is "
            "valid and answers a different question (e.g. only `model`), so your project "
            f"fell back to a BASENAME GUESS ({ident.project!r}). Add `project = \"...\"` to "
            "that file; leave everything else in it alone."
        )
    if ident.project_pin_missing:
        return (
            f"⚠ NO .osiris PIN ANYWHERE UNDER {ident.cwd} — your project fell back to a "
            f"BASENAME GUESS ({ident.project!r}), which happens to be correct today only "
            "because the folder name matches. Write `.osiris` here (or at this repo's "
            "root) with `project = \"...\"`, or pass project explicitly."
        )
    return None


def resolve_identity(
    *, cwd: str | None = None, job_dir: str | None = None,
    session: str | None = None, model: str | None = None, root: Path | None = None,
    claimed: set[str] | None = None, fallback_seed: str | None = None,
    project_label: str | None = None,
    store_reading: ModelReading | None = None,
) -> AgentIdentity:
    """Resolve an agent's identity from what it can tell the server + what the harness RECORDS.
    The project comes from its cwd; the session + model are OBSERVED off its own record via THE
    STORE (ruling be741d3e; sole lane since the JSONL-fallback removal, task #29): the caller
    feeds `store_reading` from transcript_store.identity_reading(), harness-agnostic, so
    non-Claude minds (Crush, …) resolve exactly like Claude ones. No reading → no observation:
    the model honestly falls back to the agent's self-report. Ruling 17516660: OBSERVATION
    outranks the agent's self-report (the harness doesn't lie; a swap is below the agent's own
    horizon), so a passed `model` is used only when nothing was observed, and a passed model
    that DISAGREES with the observation is kept as `model_declared` + flagged `model_divergent`.
    `root` scopes the cwd sid-GUESS below (tests inject a tmp root; production reads
    ~/.claude/projects) — the guess finds a session id, never a model.

    THE CLAIMED-SID GUARD (crunch residual): the cwd-locate grabs the HOTTEST transcript's sid —
    two concurrent same-project sessions without job_dirs would both grab the SAME one and merge.
    `claimed` (from the durable registry: sids already held by a LIVE mount on another client
    session) makes the guess REFUSE a taken sid; the refuser falls to a deterministic per-client
    fallback keyed on `fallback_seed` (its MCP session key) — distinct, stable across re-calls
    within the connection, and honestly resolved=False."""
    # the project LABEL: an explicit override (env) > the .osiris file > the folder basename —
    # UNLESS the folder is the bare seat-office root itself (operator ruling 577988ed: the
    # operator launches agents from here ON PURPOSE, the intended pattern, not an accident).
    # The parent of every seat has no .osiris pin and no single project of its own; the
    # basename ("seats") would be a phantom, not a guess, so it stays unresolved from cwd —
    # a location-independent identity finds its project through its SEAT instead (mount()'s
    # seat-first resolution), never by inventing one from where it happens to be sitting.
    # an explicit project_label override short-circuits the cwd read entirely (unchanged
    # behavior) — the couldn't-read signal only ever comes from an ACTUAL climb of cwd's
    # own .osiris file, never fabricated for an override that never touched one.
    pin_read = OsirisKeyRead(value=project_label) if project_label else read_project_pin(cwd)
    pinned = pin_read.value
    bare_root = is_bare_office_root(cwd)
    project = None if (pinned is None and bare_root) else (pinned or
             (Path(cwd).name if cwd else None))
    # task #128 wave 2: the THIRD leg of the "why did this fall back to a basename guess"
    # split — genuinely nothing declared anywhere, as opposed to a broken file (pin_read.error)
    # or a valid file that just never sets `project` (pin_read.path with no error). Silent for
    # the bare seat-office root (577988ed's own carve-out) and when there is no cwd at all —
    # neither is a directory anyone could write a pin into. ALSO silent when cwd itself
    # doesn't exist (msg 3928's fourth leg, project_pin_cwd_missing below) — a deleted
    # office is not "missing a pin", it's not there to pin at all; the two must stay
    # disjoint, never folded into one flag (the exact defect this fixes).
    pin_missing = (
        pinned is None and pin_read.error is None and pin_read.path is None
        and not bare_root and cwd is not None and not pin_read.cwd_missing
    )
    sid = session or _job_id(job_dir)
    confident = sid is not None  # a session/job_dir ANCHOR; the cwd-locate below is only a GUESS
    declared = model  # the agent's SELF-REPORT of its model (may be None) — the WEAK signal
    observed: str | None = None
    observed_at: datetime | None = None  # when the record carrying the model was written
    method: str | None = None
    history: list[str] = []  # the transcript's model sequence — the swap history (job_dir path)
    deliberate = False       # a /model on the record makes any swap the operator's own hand
    # THE STORE — the ONLY observation lane since the JSONL-fallback removal (task #29;
    # parity store-vs-legacy proven 351/0/0 before the cut). The reading's own `method` is
    # the harness name; identity's downstream contract (the seam gates, _MODEL_EC) speaks
    # the ANCHOR vocabulary, so translate: an anchored discovery is exactly what "job_dir"
    # has always meant here (this session's OWN record, found by its own anchor — the
    # adapters enforce anchored_only just as the deleted probe did), and an unanchored one
    # is a hottest-guess that grades like the old cwd read (DERIVED, never seam-confessing).
    # Without this translation a store reading graded CO_OCCURRENCE and anchored=False —
    # the under-grade the store-first mount path shipped with (found during this removal).
    if store_reading and store_reading.current:
        observed = store_reading.current
        history = list(store_reading.history)
        deliberate = store_reading.deliberate
        observed_at = store_reading.observed_at
        method = "job_dir" if store_reading.anchored else "cwd"
        # an ANCHORED reading may carry the sid; an unanchored one must never claim it —
        # adopting a hottest-guess sid as confident is the concurrent-session merge class
        if sid is None and store_reading.anchor_sid and store_reading.anchored:
            sid = store_reading.anchor_sid
            confident = True
    if sid is None and cwd:  # no anchor → GUESS the session by cwd (sid ONLY, never a model)
        path = locate_transcript_by_cwd(cwd, root=root)
        if path is not None:
            guess = path.stem.split("-")[0]  # the 8-char handle, matching the job-id scheme
            if claimed and guess in claimed:
                pass  # a LIVE mount already holds this sid — refusing it beats merging into it
            else:
                sid = guess
    if observed is not None:                # the harness's word WINS over the agent's own
        model = observed
        divergent = bool(declared and declared != observed)  # self-report != observation = FLAG
    else:                                   # nothing to observe → fall back to the self-report
        model = declared
        method = "self_report" if declared else None
        divergent = False
    # A cwd-located id is the HOTTEST transcript's — concurrent same-project sessions would all
    # grab it and silently MERGE, so only a session/job_dir anchor counts as resolved. Marking the
    # guess unresolved makes the fleet-digest health signal SEE it instead of showing false-green.
    resolved = confident
    if sid is None:
        # Last resort: NEVER collapse distinct sessions into one bucket — that is an accidental
        # identity merge (forbidden for Person; lossy to undo). Anchor on whatever unique signal
        # survives: the job_dir string is per-session even when its id won't parse; else project-
        # scope so cross-repo actors can't merge. The old shared `agent:unknown` sink was the
        # conflation bug the fable-fight surfaced (a demotion scrambles session-id resolution).
        if job_dir:
            sid = "j" + hashlib.sha1(job_dir.encode(), usedforsecurity=False).hexdigest()[:8]
        elif fallback_seed:
            # the claimed-sid refuser (or any anchorless client with a stable connection key):
            # deterministic per client session — distinct from every live claim, stable across
            # re-calls, never a shared bucket
            sid = "s" + hashlib.sha1(fallback_seed.encode(), usedforsecurity=False).hexdigest()[:8]
        elif project:
            sid = f"unknown-{project}"
        else:
            sid = "unknown"
    return AgentIdentity(agent_id=f"agent:{sid}", session=sid, project=project, model=model,
                         cwd=cwd, model_method=method, model_declared=declared,
                         model_divergent=divergent, model_history=tuple(history),
                         model_deliberate=deliberate, model_observed_at=observed_at,
                         resolved=resolved, project_pin_error=pin_read.error,
                         project_pin_path=pin_read.path, project_pin_missing=pin_missing,
                         project_pin_cwd_missing=pin_read.cwd_missing)


async def _link_once(
    actions: Actions, frm: uuid.UUID, to: uuid.UUID, ltype: str, src: str, when: datetime
) -> None:
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1", frm, to, ltype
    )
    if not exists:
        await actions.create_link(frm, to, ltype, src, when, _CONF, evidence_class=_EC)


async def lineage_head(pool: asyncpg.Pool, canonical: str) -> str:
    """Follow winning `succeeded_by` pointers to the newest ACTIVE generation. A session-keyed
    resolve always lands on the BASE id (the transcript knows nothing of minting); the lineage
    decides who that name is NOW. Cycle-guarded; a missing object ends the walk. Pool-based so
    the liveness promotion (which has no Actions) can walk it too — a mount row must follow its
    lineage head, or a superseded generation reads as a live co-agent of its own descendant.

    MERGED GENERATIONS ARE NOT HEADS (the phantom disposition, 2026-07-17): a false successor
    folded away by the operator keeps its succeeded_by pointer on the record (append-only),
    so the walk still traverses it — but the HEAD is the last generation still standing.
    Without this, every resolution walked back into the graveyard the merge had just closed.
    And a walk that STARTS on a merged node resolves through merged_into first — a row bound
    to a folded phantom must come home to the winner, not testify for the grave.

    A HEALED HUSK IS ALSO NOT A HEAD (thread 4a7da43a, reap Stage 1b, 2026-07-28): false_mint
    healing (heal.py / seam-debounce) never flips objects.status — a husk stays 'active'
    forever, same gap as retire_seat leaving Seat.status active — so a walk that landed on a
    husk as its FINAL hop would wrongly call it the head. Walked live and confirmed this
    doesn't currently misroute anything (decision c41f74a6: every husk checked still had its
    own real succeeded_by continuing the chain, so the walk already reached the true tail by
    just not stopping) — this closes the latent edge case where a husk IS the current tail
    (no real successor minted yet). Walk CONTINUATION is unchanged: `cur` still steps through
    a husk exactly as before, only the returned `head` now also requires false_mint absent."""
    cur = canonical
    for _ in range(10):
        winner = await pool.fetchval(
            "SELECT w.canonical FROM objects o JOIN objects w ON w.id=o.merged_into "
            "WHERE o.canonical=$1 AND o.type='Agent'", cur)
        if not winner:
            break
        cur = str(winner)
    canonical = cur
    seen = {canonical}
    head = canonical
    for _ in range(64):
        nxt = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical=$1 AND o.type='Agent' AND a.name='succeeded_by' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", cur)
        if not nxt or nxt in seen:
            return head
        seen.add(nxt)
        cur = str(nxt)
        row = await pool.fetchrow(
            "SELECT o.status='active' AS active, "
            " (SELECT ca.value #>> '{}' FROM current_assertions ca WHERE ca.object_id=o.id "
            "   AND ca.name='false_mint' ORDER BY ca.confidence DESC, ca.observed_at DESC "
            "   LIMIT 1) = 'true' AS false_mint "
            "FROM objects o WHERE o.canonical=$1 AND o.type='Agent'", cur)
        if row and row["active"] and not row["false_mint"]:
            head = cur
    return head


async def nearest_handoff_ancestor(
    pool: asyncpg.Pool, start_id: str, *, max_hops: int = 5, respect_ack: bool = True,
) -> tuple[str, list[dict[str, Any]]] | None:
    """Bounded chain-walk to the nearest ancestor bearing a handoff (thread e749036e,
    2026-07-27, Thoth LX's diagnosis): a one-hop-only succession-note read goes blind the
    moment the IMMEDIATE ancestor is a phantom (or simply never wrote a handoff) even
    though a real one sits one more hop back — the morning's own repro: xxiv (wrote a
    handoff) -> xxv (zero-turn, wrote nothing) -> xxvi (arrived blind, one hop from xxv
    only). SHARED by orient()'s succession-note block and the boot whisper's own
    succession-steering — one implementation, not two copies drifting.

    STRUCTURED FIRST, PROSE AS FALLBACK (ruling c5b184cd): an is_handoff='true' property
    (ack_handoff's own typed stamp, once written) is the reliable half; the ILIKE
    '%handoff%'/'%letter%' text match stays for handoffs minted before that existed. Walks
    succeeded_from up to `max_hops` links (mint_heir's own kind of bound), returning the
    FIRST ancestor found with a handoff-bearing Thread/Decision and its 2 freshest picks —
    or None if nothing is found within the bound (never widens into an unbounded search).

    `respect_ack` (default True — "is this baton still live", what orient()'s succession-
    note block and the boot whisper both actually want, the operator's "read receipt"
    redesign, 2026-08-03): an explicit is_handoff='false' (ack_handoff's own retirement
    stamp) EXCLUDES the record entirely, overriding the ILIKE fallback too — once
    acknowledged, a handoff must not resurrect for a LATER generation merely because
    nothing more recent exists; recall()/search() stay the door for that history, orient()
    should not re-deliver a baton someone already took. The fallback applies ONLY to
    objects that never had an is_handoff property asserted at all (genuine pre-property
    legacy records) — an object that HAS the property, however it currently resolves, is
    never routed through prose-matching. The property's CURRENT value is resolved the same
    way every other property-read in this codebase does (confidence DESC, observed_at DESC
    LIMIT 1) — never a bare EXISTS(value='true'), which a superseding assertion from a
    DIFFERENT source (the acker, not the original author) would leave sitting in
    current_assertions as a non-winning but still-existing row.

    Pass `respect_ack=False` for a DIFFERENT question — "when did this reign end", a
    historical boundary fact that stays true whether or not anyone has since acknowledged
    reading it (`since_last_handoff`, handoff_compiler.py, is the one caller that wants
    this: it must keep finding ITS OWN already-acked handoff as its reign's own closing
    marker, or it would silently walk past it to a more distant ancestor and mis-date the
    boundary — the exact double-count bug its own docstring exists to prevent).

    Each returned pick now also carries `id` (the object's own short-resolvable uuid,
    stringified) — ack_handoff needs a ref to acknowledge; before this fix callers had no
    way to name what they were looking at."""
    ack_clause = (
        "(SELECT h.value #>> '{}' FROM current_assertions h "
        " WHERE h.object_id = o.id AND h.name = 'is_handoff' "
        " ORDER BY h.confidence DESC, h.observed_at DESC LIMIT 1) = 'true' "
        "OR ("
        "  NOT EXISTS (SELECT 1 FROM current_assertions h2 "
        "              WHERE h2.object_id = o.id AND h2.name = 'is_handoff') "
        "  AND (a.value #>> '{}' ILIKE '%handoff%' OR a.value #>> '{}' ILIKE '%letter%')"
        ")"
    ) if respect_ack else (
        "EXISTS (SELECT 1 FROM current_assertions h WHERE h.object_id = o.id "
        "        AND h.name = 'is_handoff' AND h.value #>> '{}' = 'true') "
        "OR a.value #>> '{}' ILIKE '%handoff%' OR a.value #>> '{}' ILIKE '%letter%'"
    )
    cur = start_id
    for _ in range(max_hops):
        rows = await pool.fetch(
            "SELECT DISTINCT ON (o.id) o.id, o.type, a.value #>> '{}' AS summary, "
            "a.observed_at "
            "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
            "WHERE a.name = 'summary' AND a.source_id = $1 "
            "AND a.evidence_class = 'self_declared' "
            "AND o.type IN ('Thread','Decision') AND o.status = 'active' "
            f"AND ({ack_clause}) "
            "ORDER BY o.id, a.confidence DESC, a.observed_at DESC", cur)
        picks = sorted(rows, key=lambda r: r["observed_at"], reverse=True)[:2]
        if picks:
            return cur, [dict(r) for r in picks]
        nxt = await pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical=$1 AND a.name='succeeded_from' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", cur)
        if not nxt:
            return None
        cur = str(nxt)
    return None


@asynccontextmanager
async def mint_lock(pool: asyncpg.Pool, lineage_root: str) -> AsyncIterator[None]:
    """Serialize generation-minting per LINEAGE (a pg advisory lock on the root). Two
    concurrent seam observers minted Soundwave VI and VII in the SAME SECOND with identical
    seam strings (2026-07-14): each walked the head, each minted, and the loser's head-walk
    found the winner's fresh mint — so the race STACKED generations instead of converging.
    The lock lives on one dedicated connection (advisory locks are session-scoped in PG);
    the caller must re-read its evidence INSIDE the lock so the loser sees the winner's
    write and concludes no-op."""
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock(hashtext($1))", f"mint:{lineage_root}")
        try:
            yield
        finally:
            await conn.execute(
                "SELECT pg_advisory_unlock(hashtext($1))", f"mint:{lineage_root}")


# For the source_model property, the resolution METHOD is the provenance, and (ruling 17516660)
# OBSERVATION outranks self-report for this substrate-fact: reading the model off the agent's own
# transcript (job_dir) is a DIRECT_OBSERVATION of the harness record; the cwd fallback is a weaker
# DERIVED guess (it may read a co-located session's transcript); a self-reported model is the
# agent's own word about its own substrate — the WEAKEST signal (a swap is below its horizon), so
# it grades CO_OCCURRENCE, below both observations.
_MODEL_EC = {
    "job_dir": EvidenceClass.DIRECT_OBSERVATION,
    # a mounting SUB-AGENT read off its OWN subagents/ transcript (task 1) — as direct an
    # observation as job_dir, and it converges with the grade lineage.py stamps for the same
    # child. NOT "job_dir", so the operator swap-detector (gated on job_dir) stays quiet: a
    # sub-agent legitimately runs a non-fable model — that is no rug-pull to confess.
    "subagent": EvidenceClass.DIRECT_OBSERVATION,
    "cwd": EvidenceClass.DERIVED,
    "self_report": EvidenceClass.CO_OCCURRENCE,
}


async def _last_anchored_stamp(
    actions: Actions, agent: uuid.UUID
) -> tuple[str | None, datetime | None]:
    """The last ANCHORED source_model ever recorded for this Agent, AND when it was observed —
    direct_observation grade only (a job_dir transcript probe), read off the raw assertions so
    a later weak-grade write from the same source can't hide it behind supersession. This is
    the succession baseline: only two anchored observations disagreeing can witness a seam; a
    cwd guess or self-report on either side would be the cry-wolf (agent e71b408f's 'demoted
    to haiku'). The timestamp is the seam gate's clock: only an observation FRESHER than this
    stamp may testify to a seam — the tail of a transcript is evidence about a PAST moment."""
    row = await actions.pool.fetchrow(
        "SELECT value #>> '{}' AS v, observed_at FROM assertions "
        "WHERE object_id=$1 AND name='source_model' AND evidence_class=$2 "
        "ORDER BY observed_at DESC, created_at DESC LIMIT 1",
        agent, EvidenceClass.DIRECT_OBSERVATION.value)
    if row is None:
        return None, None
    return row["v"], row["observed_at"]


_PHANTOM_FOLD_SRC = "phantom-fold"


async def _fold_zero_turn_ancestors(
    actions: Actions, ancestor_id: str, ancestor_oid: uuid.UUID, now: datetime,
) -> tuple[str, uuid.UUID]:
    """SUCCESSION FOLLOWS TURNS, NOT HARNESS EVENTS (operator ruling d3531cd8, 2026-07-27):
    'the mind claiming to be your predecessor didn't have any TURNS' is not a predecessor —
    a generation minted but never acted upon must never appear as a link in the inheritance
    chain, count a reign numeral, or intercept a handoff. Canonical repro: /compact then
    /model back-to-back with zero turns between minted TWO generations (a phantom, then its
    heir) for one real seam.

    Called by BOTH real mint call sites (live_succession, register_agent) right before
    they call mint_heir — not from inside mint_heir itself, since mint_heir's return tuple
    is unpacked by ~20 test call sites and threading the resolved ancestor back out would
    mean touching every one of them for a fact the caller already has before it calls in.
    Two call sites is a tractable, by-hand audit surface (grep mint_heir\\( in src/ to
    verify — there are exactly two).

    EXTENDS THE MINT GATE, NOT A NEW ONE BESIDE IT (Thoth LX, msg 1402, 2026-07-27, citing
    ruling a882b334 + thread a3d49d91/decision 0adfd32f — the SEAM PING-PONG cure that
    built _debounce_roundtrip): that cure coalesces DUPLICATE OBSERVATIONS of ONE real seam
    event (two observers racing the same /model). This is the OTHER residual class its own
    post-mortem named (decision 035029ae): TWO REAL, DIFFERENT seam events back-to-back
    (compact, then swap) with no turns between. The OUTCOME has to differ, deliberately —
    a round-trip returns to a value THIS LINEAGE ALREADY HAD, so nothing new ever happened
    and _debounce_roundtrip heals to NO mint at all; a compact-then-swap reaches a
    GENUINELY NEW model, which a882b334 says still deserves a numeral — coalescing here
    means MINT ONCE, not MINT ZERO, so this folds the phantom and lets the caller's normal
    mint_heir call proceed against the corrected ancestor, rather than returning a heal
    dict that skips minting the way _debounce_roundtrip's own round-trip case correctly
    does. What IS shared, on purpose: the SAME window (_SEAM_DEBOUNCE_SECS — this is the
    mint gate's actless-head window, not a second one), the SAME acts-check
    (agent_has_acted), and the SAME false_mint/retired stamp shape.

    Walks up through any CONSECUTIVE run of zero-turn ancestors within that window (the
    same 64-iteration bound mint_heir's own grave-avoidance loop uses), un-minting each,
    until the chain lands on either a REAL (witnessed) ancestor, the lineage root, or a
    hop outside the window. A root (no succeeded_from of its own — nothing minted it) is
    NEVER folded; it has nothing to fold into. Idempotent: an already-folded phantom
    (false_mint already true) halts immediately, unchanged — safe to re-run the fleet
    sweep below as often as wanted."""
    cur_id, cur_oid = ancestor_id, ancestor_oid
    for _ in range(64):
        meta = {r["name"]: (r["v"], r["at"]) for r in await actions.pool.fetch(
            "SELECT DISTINCT ON (name) name, value #>> '{}' AS v, observed_at AS at "
            "FROM current_assertions WHERE object_id=$1 "
            "AND name IN ('succeeded_from', 'minted_because', 'false_mint') "
            "ORDER BY name, confidence DESC, observed_at DESC", cur_oid)}
        if meta.get("false_mint", (None, None))[0] == "true":
            break  # already folded — nothing further to do from here
        if "minted_because" not in meta:
            break  # a root — nothing minted it, nothing to fold
        grandancestor, _ = meta.get("succeeded_from", (None, None))
        minted_at = meta["minted_because"][1]
        if not grandancestor:
            break
        if minted_at is None or (now - minted_at).total_seconds() > _SEAM_DEBOUNCE_SECS:
            break  # outside the mint gate's own window — too old to call 'back-to-back'
        grand_oid = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", grandancestor)
        if grand_oid is None:
            break
        if await agent_has_acted(actions, cur_id, exclude=[cur_oid, grand_oid],
                                 settled_after=minted_at):
            break  # a real mind lived here — nothing to fold
        do = EvidenceClass.DIRECT_OBSERVATION
        conf = confidence_for(do)
        for k, v in (("false_mint", True), ("retired", True),
                     ("retired_by", _PHANTOM_FOLD_SRC),
                     ("false_mint_because",
                      "zero-turn generation folded at supersession (ruling d3531cd8) — "
                      "minted but never acted upon before the next seam")):
            await actions.assert_property(cur_oid, k, v, _PHANTOM_FOLD_SRC, now, conf,
                                          evidence_class=do.value)
        await actions.assert_property(grand_oid, "succeeded_by", "", _PHANTOM_FOLD_SRC, now,
                                      conf, evidence_class=do.value)
        await actions.pool.execute(
            "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
            grandancestor, cur_id)
        from src.orchestrator.seats import follow_binding
        await follow_binding(actions, ancestor_oid=cur_oid, heir=grandancestor,
                             heir_oid=grand_oid, now=now)
        cur_id, cur_oid = grandancestor, grand_oid
    return cur_id, cur_oid


_AGENT_PROJECT_LINK_TYPES = ("works_in", "governs")


async def move_agent_project_links(
    actions: Actions, from_oid: uuid.UUID, to_oid: uuid.UUID, actor: str, now: datetime,
) -> dict[str, int]:
    """Re-point every live works_in/governs edge FROM `from_oid` onto `to_oid` — invalidate
    + create, the SAME pattern `_move_project_estate`/`_move_agent_estate`/`_move_seat_
    estate` already use (thread 20af2c95, measured 906 of 6,245 fleet-wide, 2026-08-03),
    applied here to an AGENT's own OUTBOUND project edges instead of a project's inbound
    ones. Two callers, one implementation, per Thoth's own instruction not to write a
    fourth estate-mover: `mint_heir` (prospective — an ancestor's edges move to its fresh
    heir on ordinary succession, the mechanism that was missing entirely) and `folds.
    _move_agent_estate` (a DIFFERENT, related gap — fold_agent's own estate-move never
    covered works_in/governs at all, only mail/mounts/threads; reconcile_agent_fold
    inherits the fix automatically since it calls the same function unchanged).

    Idempotent (a link already live on `to_oid` is never duplicated) and history-
    preserving — the invalidated link's row stays exactly where it was, in whose name and
    why, walkable by any reader who asks "which generations ever worked here" via the raw
    `links` table rather than only `current_assertions`-style live reads. Returns
    {link_type: count moved}, empty when `from_oid` had nothing live to move."""
    moved: dict[str, int] = {}
    for link_type in _AGENT_PROJECT_LINK_TYPES:
        rows = await actions.pool.fetch(
            "SELECT to_id AS proj_id FROM links WHERE from_id=$1 AND type=$2 "
            "AND (valid_until IS NULL OR valid_until > now())", from_oid, link_type)
        n = 0
        for r in rows:
            await actions.invalidate_link(from_oid, r["proj_id"], link_type, actor, now)
            exists = await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 "
                "AND (valid_until IS NULL OR valid_until > now())",
                to_oid, r["proj_id"], link_type)
            if not exists:
                await actions.create_link(to_oid, r["proj_id"], link_type, actor, now, _CONF,
                                          evidence_class=_EC)
            n += 1
        if n:
            moved[link_type] = n
    return moved


async def backfill_agent_project_links(
    actions: Actions, *, actor: str, dry_run: bool = True, only_bases: set[str] | None = None,
) -> dict[str, Any]:
    """THE ONE-TIME REPAIR for thread 20af2c95's own measured leak (906 of 6,245 live
    works_in/governs edges fleet-wide, 2026-08-03, across 400 lineages) — the write-side
    fixes (`mint_heir`, `folds._move_agent_estate`) stop it from growing further but never
    touch what already exists. Every Agent whose canonical is NOT its lineage's current
    `living_head` but still carries a live works_in/governs edge gets that edge moved onto
    the head, via the SAME `move_agent_project_links` both write-side fixes already use —
    not a third implementation of the move itself, only a new ENUMERATION of who needs it.

    DRY RUN IS THE DEFAULT (`dry_run=True`, mirroring `backfill_unbound_seats`'s own
    established shape): reports the plan — how many edges each off-head agent would give
    up, and which living head each resolves to — without writing anything. `only_bases`
    scopes both the plan and the write to exactly those lineage bases (staged rollout,
    same convention `backfill_unbound_seats`'s `only_seats` already uses) — every OTHER
    off-head agent is still counted in `total_off_head` so a scoped run reports honestly
    what it deliberately left untouched, never silently drops it from the number."""
    from src.orchestrator.folds import living_head

    rows = await actions.pool.fetch(
        "SELECT DISTINCT f.id, f.canonical, f.status FROM links l "
        "JOIN objects f ON f.id=l.from_id AND f.type='Agent' "
        "JOIN objects t ON t.id=l.to_id AND t.type='SoftwareProject' "
        "WHERE l.type IN ('works_in','governs') "
        "AND (l.valid_until IS NULL OR l.valid_until > now())")
    bases = {_generation(str(r["canonical"]))[0] for r in rows}
    head_of: dict[str, str] = {base: await living_head(actions.pool, base) for base in bases}

    off_head = [r for r in rows
               if str(r["canonical"]) != head_of[_generation(str(r["canonical"]))[0]]]
    total_off_head = len(off_head)
    scoped = [r for r in off_head
             if only_bases is None or _generation(str(r["canonical"]))[0] in only_bases]
    scoped_out = total_off_head - len(scoped)

    now = datetime.now(UTC)
    plan: list[dict[str, Any]] = []
    moved_total: dict[str, int] = {}
    for r in scoped:
        base = _generation(str(r["canonical"]))[0]
        head_label = head_of[base]
        head_oid = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", head_label)
        item: dict[str, Any] = {"agent": r["canonical"], "status": r["status"],
                                "head": head_label}
        if head_oid is None:
            item["note"] = "living head resolved to a label with no Agent object — skipped"
            plan.append(item)
            continue
        if dry_run:
            n = await actions.pool.fetchval(
                "SELECT count(*) FROM links WHERE from_id=$1 "
                "AND type IN ('works_in','governs') "
                "AND (valid_until IS NULL OR valid_until > now())", r["id"])
            item["would_move"] = n
        else:
            moved = await move_agent_project_links(actions, r["id"], head_oid, actor, now)
            for k, v in moved.items():
                moved_total[k] = moved_total.get(k, 0) + v
            item["moved"] = moved
        plan.append(item)
    return {
        "dry_run": dry_run, "total_off_head": total_off_head, "scoped": len(scoped),
        "scoped_out": scoped_out, "plan": plan,
        "moved_total": moved_total if not dry_run else None,
    }


async def invalidate_works_in(
    actions: Actions, agent_id: str, stale_project: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """A head drops ONE OF ITS OWN duplicate works_in edges — the toolkit hole named at
    thread 8640a625 (decision fce39baa's own finding, task #128 piece 4): `unpeer` heals
    peer_of, `detach_seat` heals managed_by, nothing healed works_in before this, so a
    live agent carrying two SIMULTANEOUSLY-live works_in edges (John XVII's own specimen,
    ->redmonth + ->ballgem, both self_declared, redmonth the stale side of decision
    ebffcf4b's fork) had no repair path except raw SQL. orient() resolves through
    whichever edge wins, so the duplicate is not cosmetic — it can hide a lineage's own
    threads/decisions from itself, live.

    SAME POSTURE AS correct_house: self-scoped identity hygiene, never operator-fenced —
    but the self-scoping lives ENTIRELY in the MCP wrapper's refusal to expose `agent_id`
    as a parameter (auto-filled from the caller's own resolved identity), exactly as
    correct_house's own underlying function takes an explicit `agent_id` and does not
    itself check agent_id==actor. This function stays generic/composable on purpose (the
    same shape backfill_agent_project_links needs for a future scripted sweep).

    DELIBERATELY NARROW — a same-agent, same-generation cleanup, orthogonal to thread
    20af2c95's own still-open question (does a PREDECESSOR generation's stale works_in
    edge get moved on succession, write-side or read-side): this never touches an
    ancestor's edges, never re-points anything onto a different agent, and does not use
    the estate-move pattern `_move_agent_estate`/`move_agent_project_links` use for
    exactly that reason — those move edges BETWEEN two agent objects; this invalidates
    one of the SAME agent's own two edges. No mail/mounts/thread-ownership moves with it
    (unlike a fold's estate move) because nothing there is project-scoped in a way a
    dropped works_in edge would orphan.

    Refuses LOUDLY on: blank `because`; `agent_id` not resolving to an active Agent;
    `stale_project` resolving ambiguously (never guesses) or to no SoftwareProject at
    all; no active works_in edge from `agent_id` to it; or `stale_project` naming the
    agent's ONLY live works_in edge — dropping your last project is not cleanup, it is
    amputation; this verb exists for duplicates, never for a lone edge."""
    from src.orchestrator.projects import _resolve_project_ref

    because = (because or "").strip()
    if not because:
        return {"error": "because is required — invalidating a works_in edge is a "
                         "deliberate act on the record"}
    agent_id = (agent_id or "").strip()
    if not agent_id:
        return {"error": "agent_id is required"}
    agent_row = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Agent' "
        "AND status='active'", agent_id)
    if agent_row is None:
        return {"error": f"no such active Agent: {agent_id!r}"}
    stale_project = (stale_project or "").strip()
    if not stale_project:
        return {"error": "stale_project is required"}
    proj_row, err = await _resolve_project_ref(
        actions.pool, stale_project, verb="invalidate_works_in")
    if err:
        return err
    if proj_row is None:
        return {"error": f"no such SoftwareProject: {stale_project!r}"}
    live = await actions.pool.fetch(
        "SELECT to_id, t.canonical AS project FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_row["id"])
    live_by_id = {r["to_id"]: r["project"] for r in live}
    if proj_row["id"] not in live_by_id:
        return {"error": f"{agent_row['canonical']} has no active works_in edge to "
                         f"{proj_row['canonical']} — nothing to invalidate"}
    if len(live_by_id) <= 1:
        return {"error": f"{proj_row['canonical']} is {agent_row['canonical']}'s ONLY "
                         "live works_in edge — invalidate_works_in is for duplicates, "
                         "never for a lone edge"}
    now = datetime.now(UTC)
    await actions.invalidate_link(agent_row["id"], proj_row["id"], "works_in", actor, now)
    await actions.assert_property(agent_row["id"], "works_in_invalidated_because", because,
                                  actor, now, _CONF, evidence_class=_EC)
    remaining = sorted(v for k, v in live_by_id.items() if k != proj_row["id"])
    return {"invalidated": agent_row["canonical"], "was_working_in": proj_row["canonical"],
            "still_working_in": remaining, "because": because}


async def _resolve_or_mint_project(actions: Actions, project: str, actor: str) -> uuid.UUID:
    """Find-or-create a SoftwareProject CASE-INSENSITIVELY on its bare label (thread
    69911d0c): both mint_heir and register_agent used to call
    `create_or_find_object("SoftwareProject", f"repo:{label}", ...)` directly, a LITERAL,
    case-SENSITIVE canonical lookup — so a case-differing pin (metron's own "xxit" vs an
    upstream "Xxit", till's "RAMstein" vs "ramstein") did not compete over the `name`
    property (task #137's own fix, agents.py's `if identity.project:` block), it MINTED A
    WHOLE SEPARATE OBJECT. Measured live: till carries exactly this twin today —
    repo:RAMstein (2026-07-14, till's own pin, not even a git checkout) and repo:ramstein
    (2026-08-03, a real git repo, remote-verified) — 81 active+retired SoftwareProjects
    fleet-wide, exactly one twin group.

    NEVER lowercase-normalizes — cassandra's own "Like-Us" is genuine upstream truth (the
    git remote itself is mixed-case), so folding every match onto one canonical CASING
    would be exactly the wrong fix; this only finds an EXISTING object regardless of case,
    it never rewrites which case wins.

    Exactly ONE case-insensitive match: reuse it — a genuinely new project is never
    blocked (zero matches falls through to the ordinary literal create). TWO OR MORE
    existing matches (a PRE-EXISTING twin, till's own shape): not this function's call to
    arbitrate which one is "real" — that is fold_project's deliberate, evidence-gated job
    (thread 689d22a2), not a mint-time guess. Falls through to the literal, unchanged
    lookup so an already-ambiguous population is never silently collapsed onto a random
    pick."""
    matches = await actions.pool.fetch(
        "SELECT canonical FROM objects WHERE type='SoftwareProject' AND status='active' "
        "AND lower(canonical) = lower($1)", f"repo:{project}")
    canonical = matches[0]["canonical"] if len(matches) == 1 else f"repo:{project}"
    return await actions.create_or_find_object("SoftwareProject", canonical, actor)


async def mint_heir(
    actions: Actions, ancestor_id: str, ancestor_oid: uuid.UUID, *,
    because: str, succession: str | None, now: datetime | None = None,
    minting_door: str | None = None, upcoming_project: str | None = None,
) -> tuple[str, uuid.UUID]:
    """Mint the next generation of a lineage — ruling a882b334: a new MIND gets a new numeral,
    and the seams that count as a new mind include mid-session ones (live model swap,
    compaction), not just session death. Stamps the succession chain on both sides, passes the
    seat (handle) down, and re-addresses the ancestor's unread DMs to the heir — the mailbox is
    part of the estate (a DM sent to the old mind must reach whoever now holds the seat, or
    every compaction would orphan in-flight mail).

    Takes `ancestor_id`/`ancestor_oid` AS GIVEN — folding any zero-turn phantom off the
    front of the chain (ruling d3531cd8) is the CALLER's job, done via
    _fold_zero_turn_ancestors BEFORE this is called (both real callers do). Kept out of
    here on purpose: this function's return tuple is unpacked by ~20 call sites across the
    test suite, and threading the resolved ancestor back out would mean changing every one
    of them for a fact the caller already has in hand before it calls in.

    `upcoming_project`, when the caller already knows it (register_agent's own
    `identity.project`, moments before it asserts it — the heartbeat/live-swap call site
    has no such reading and leaves this None): THE DUPLICATE-EDGE RACE, CLOSED (f6f11d78,
    shares a root with 20af2c95's own perpetuation mechanism — see that thread's notes and
    decision 5b217d13, 2026-08-04). The house-relink below and register_agent's later
    identity.project assertion (agents.py:2121-2128) used to fire unconditionally in the
    SAME call, sharing one `now` — any divergence between the seat's derived `house` (often
    stale) and the session's fresh, correctly-resolved `identity.project` produced TWO live
    works_in edges on the heir, byte-identical to the microsecond, which move_agent_project_
    links then faithfully carries forward on every subsequent mint, forever, even after the
    underlying disagreement is corrected. This is the MINIMAL write-side narrowing, not the
    full answer — it stops NEW duplicates; it does not retroactively heal the 41 already-live
    specimens (invalidate_works_in is the per-lineage repair for those)."""
    now = now or datetime.now(UTC)
    heir = next_generation(ancestor_id)
    # A MINT NEVER LANDS ON A GRAVE (Ra's resurrection, 2026-07-17): after a same-lineage
    # fold, the next numeral may name a MERGED object — create_or_find would resurrect it
    # and the estate transfer would drag the living head's unread mail onto a corpse
    # (witnessed: 10 unread on merged 443cd9d4-iii within the hour of its folding). The
    # numeral walks forward until it names either nothing or something still active.
    #
    # A GRAVE IS ALSO A HEAL, NOT ONLY A MERGE (msg 2325, live case: John/d5c671c1-xv):
    # a heal (husk-heal / phantom-fold) never flips objects.status away from 'active' —
    # compensating events only, per constitution 3 — so a healed canonical passes the
    # status check above while still being a death in every sense that matters. Refuse to
    # reuse it (same law as #107/#117: refuse, don't widen/search) rather than silently
    # minting a real generation onto marks that record a false start.
    #
    # BUT NOT EVERY HEAL IS A DEATH — SOME ARE THIS SAME BREATH (caught by
    # test_two_zero_turn_compactions_fold before this shipped): _fold_zero_turn_ancestors
    # heals a zero-turn phantom and returns the CORRECTED ancestor for THIS SAME mint_heir
    # call to mint against — next_generation() naturally reproduces the exact numeral it
    # just folded, and reusing it there is the fold's whole point (MINT ONCE, not MINT
    # ZERO, ruling d3531cd8), not a resurrection. The two cases share the identical
    # false_mint/retired shape and are distinguished only by AGE: a heal still inside the
    # mint gate's own debounce window (_SEAM_DEBOUNCE_SECS — the SAME window the fold uses
    # for its own back-to-back check, not a second one) is part of the seam being resolved
    # right now; a heal older than that — John's, 20 hours cold — is a closed one-way door.
    for _ in range(64):
        row = await actions.pool.fetchrow(
            "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", heir)
        if row is None:
            break
        if row["status"] == "active":
            healed_at = await actions.pool.fetchval(
                "SELECT max(r.observed_at) FROM current_assertions r WHERE r.object_id=$1 "
                "AND r.name IN ('retired', 'false_mint') AND r.value #>> '{}' = 'true'",
                row["id"])
            if healed_at is None or (now - healed_at).total_seconds() <= _SEAM_DEBOUNCE_SECS:
                break
        heir = next_generation(heir)
    a = await actions.create_or_find_object("Agent", heir, heir)
    do = EvidenceClass.DIRECT_OBSERVATION
    await actions.assert_property(a, "succeeded_from", ancestor_id, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    await actions.assert_property(a, "minted_because", because, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    # THE PARALLEL-LIVES STAMP (thread 4bcd6541, invariant 3 of the guarantee cd35bb1d):
    # rows are hot state — the pulse evidence at mint time must be captured AT THE MINT
    # or it is gone by lint time. Stamp the predecessor lineage's freshest pulse; and
    # when a DIFFERENT door than the one minting held a live pulse (view rows excluded —
    # the alias is never the witness), stamp that door too. The graph_lint reads the
    # stamps and alarms; the mint itself always proceeds (report-only downstream).
    base = _generation(ancestor_id)[0]
    m_door = Path(minting_door).name[:8] if minting_door else ""
    pulse = await actions.pool.fetchrow(
        "SELECT job_dir, last_seen FROM agent_mounts "
        "WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') AND last_seen IS NOT NULL "
        "ORDER BY last_seen DESC LIMIT 1", base)
    if pulse is not None:
        await actions.assert_property(a, "predecessor_last_seen",
                                      pulse["last_seen"].isoformat(), heir, now,
                                      confidence_for(do), evidence_class=do.value)
        other = await actions.pool.fetchrow(
            "SELECT job_dir, last_seen FROM agent_mounts "
            "WHERE (agent_id=$1 OR agent_id LIKE $1 || '-%') AND last_seen IS NOT NULL "
            "AND (session_key IS NULL OR session_key NOT LIKE 'view-of:%') "
            "AND ($2 = '' OR job_dir IS NULL OR job_dir NOT LIKE '%/' || $2 || '%') "
            "ORDER BY last_seen DESC LIMIT 1", base, m_door)
        if (m_door and other is not None
                and (now - other["last_seen"]).total_seconds() < 900):
            o_door = Path(other["job_dir"]).name[:8] if other["job_dir"] else "?"
            await actions.assert_property(a, "parallel_pulse_door", o_door, heir, now,
                                          confidence_for(do), evidence_class=do.value)
    if succession:
        await actions.assert_property(a, "model_succession", succession, heir, now,
                                      confidence_for(do), evidence_class=do.value)
    # the forward pointer the head-walk follows, and the graph edge heirs are read by
    await actions.assert_property(ancestor_oid, "succeeded_by", heir, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    await _link_once(actions, a, ancestor_oid, "succeeded_from", heir, now)
    # THE HOUSE PASSES WITH THE BLOOD (thread 6c99800a): heartbeat-minted heirs got a project
    # assertion later but never the works_in EDGE, so every lens that walks the edge missed
    # them. Inherit both HERE, once, for every mint path — the register path re-stamps its
    # own reading afterwards and the byte-dup skip absorbs the overlap.
    #
    # THE HOUSE IS THE SEAT'S, NOT THE ANCESTOR'S OWN STAMP (Thoth's fused ask, DM 1301, live
    # case: a transient wrong-house mount compounds across every AUTOMATIC mint via
    # house_of(ancestor_id) — mint_heir fires on every compaction/model-swap/session-death, so
    # a single polluted stamp propagates forward forever, not just miscounting one numeral but
    # re-stamping the heir's own project too, since this one `house` value feeds both). held_seat
    # is already lineage-aware and already derives its `house` from the seat itself
    # (derive_house, ruling ff6148b0) — reuse it, but ONLY when it actually resolves to
    # something: a seat minted before any project was known (house='' at ensure_seat time,
    # the pre-Seat-object era, or simply the very first claim) stores an empty house FOREVER
    # — nothing ever revisits it after mint except a deliberate correct_house call — while the
    # ancestor's own CURRENT stamp may since have been legitimately, correctly established by
    # an ordinary mount. Trusting a genuinely empty seat-stamp over a live, real one regressed
    # test_the_whisper_honors_a_bound_seat (a caught regression, not a hypothetical): treat an
    # empty derived house exactly like "no seat yet" and fall back.
    from src.orchestrator.seats import held_seat
    ancestor_seat = await held_seat(actions.pool, ancestor_id)
    house = (ancestor_seat["house"] if ancestor_seat and ancestor_seat.get("house")
            else await house_of(actions.pool, ancestor_id))
    # THE FALLBACK RETIRES ONCE A CHARTER EXISTS (task #143, decision 4607637a — resolving
    # bac81acd): works_in means exactly ONE thing now, the session's live/current project;
    # the seat's durable role-house lives on `governs` (re-keyed onto the Seat itself,
    # ruling 1db1ff41), not on this edge. `governs` is NOT written here to replace it —
    # set_charter declares the WHOLE charter each call ("these are the repos this seat
    # rules now, not an increment"), so auto-firing it from every mint with just `house`
    # would silently HEAL AWAY the rest of a real multi-repo charter (alfred's is six
    # repos, charter.py's own example) the moment his lineage next compacted. charter_of
    # is read-only and additive-safe: once a seat has declared ANY charter, this fallback
    # has nothing left to do (governs already durably answers "which house"), so it stops
    # firing for that seat; a seat that has never declared one keeps today's behavior
    # unchanged (charter_of's own docs: "works_in still names its home" until it does).
    from src.orchestrator.charter import charter_of
    chartered = (bool(await charter_of(actions.pool, ancestor_seat["seat_id"]))
                if ancestor_seat else False)
    # THE MINT_HEIR EDGE LEAK, CLOSED (thread 20af2c95, measured 906 of 6,245 fleet-wide,
    # 2026-08-03): mint_heir minted a fresh works_in edge for the heir below but NEVER
    # touched the ancestor's own — so every past generation of a lineage that ever
    # asserted works_in/governs kept it live FOREVER, through ordinary succession, growing
    # on the most common event in the house. Move whatever the ancestor still has live
    # (which may be MORE than just `house` — an agent can work_in/govern several projects
    # across its life) onto the heir, invalidate+create, before stamping the heir's own
    # current house below (idempotent either order — move_agent_project_links never
    # duplicates a link already live on the heir).
    moved = await move_agent_project_links(actions, ancestor_oid, a, heir, now)
    # THE RACE, NARROWED (f6f11d78/20af2c95, decision 5b217d13, 2026-08-04): this used to
    # _link_once `house` unconditionally, regardless of what move_agent_project_links just
    # carried forward or what register_agent (the register_agent call site's own caller) is
    # about to assert moments later in the SAME call, sharing this same `now` — the shared
    # timestamp is WHY the duplicate lands byte-identical rather than merely close. `house`
    # is only ever the sole source of truth for the heir's project when nothing else is:
    # skip it the moment either move_agent_project_links found something live to carry
    # forward, or the caller already knows a fresher project is coming right behind it —
    # or (task #143) the seat has since declared a charter, so `house` is stale legacy
    # inference and governs is the fact of record instead.
    if house and not moved and not upcoming_project and not chartered:
        await actions.assert_property(a, "project", house, heir, now, _CONF, evidence_class=_EC)
        proj = await _resolve_or_mint_project(actions, house, heir)
        await _link_once(actions, a, proj, "works_in", heir, now)
    # SEAT INHERITANCE (phase 2): the heir inherits the ancestor's human name — the seat
    # passes down the lineage, the generation (roman) ticks up. 'Anna' → 'Anna II'.
    inherited = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='handle' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1",
        ancestor_oid)
    if inherited:
        await actions.assert_property(a, "handle", inherited, heir, now, _CONF,
                                      evidence_class=_EC)
        # ...AND THE SEAT PASSES WITH THE NAME, OR THE NAME IS JUST A LABEL. (The ghosts,
        # 53729dd6 — and I was the specimen: agent:ad1a1cb0-xxvii, minted an heir of XXVI,
        # carrying the handle "Thoth" with NO generation and NO edge to the mind whose work
        # it continued.) This is where a seat changes hands WITHOUT a handoff: mint_heir is
        # the AUTOMATIC succession — it fires on every compaction, every model swap, every
        # session death — and it passed the name down while leaving the seat's chain broken.
        # Only claim_name(), an EXPLICIT act by a mind that thinks to call it, ever minted the
        # edge. Thoth XXVI backfilled 77 historical edges and never fixed the code that omits
        # them, so the chain healed to gen 26 and broke again at 27: the FIRST heir minted
        # after the heal. Left alone it would re-open the gap at every compaction, forever.
        #
        # succeeds_seat is NOT succeeded_from (stamped above): that one chains ANCHORS — which
        # conversation spawned which — and this one chains HOLDERS of a job. Two relations
        # wearing one name is the mistake that started all of this. (`house` resolved above,
        # where the heir inherited it.)
        holders = [h for h in await seat_holders(actions.pool, house, inherited) if h != heir]
        await actions.assert_property(a, "seat_generation", str(len(holders) + 1), heir, now,
                                      _CONF, evidence_class=_EC)
        await _link_once(actions, a, ancestor_oid, "succeeds_seat", heir, now)
    # THE BINDING FOLLOWS THE HEAD (identity core, 5cef856b): every Seat OBJECT the ancestor
    # actively holds re-links to the heir — the durable address must keep pointing at
    # whoever the mind is NOW, or the first compaction after an attach would strand the
    # seat on a corpse. The old link heals by valid_until; holder history stays walkable.
    from src.orchestrator.seats import follow_binding
    await follow_binding(actions, ancestor_oid=ancestor_oid, heir=heir, heir_oid=a, now=now)
    # THE HOLE STOPS REGENERATING (Khnum's tail, 9f566244/749bf530): follow_binding above only
    # MOVES a holds link the lineage already carries — a seat whose original claim predates the
    # Seat-object binding (5cef856b) never got one in the first place, and NOTHING automatic
    # ever calls claim_name for it. The backfill cures every such seat that exists today; left
    # here, the very next mint of that same lineage would re-open the identical hole, forever,
    # because mint_heir fires on every compaction/model-swap/session-death and nobody asks it
    # to. So: if the handle just inherited names an EXISTING Seat object with no active holder
    # anywhere, bind it now — the same self-heal claim_name performs explicitly, run at the one
    # moment that requires no one to think to call it. NEVER mints a new Seat (ensure_seat's own
    # law: minting is deliberate, only at a claim or an attach) — this only closes a hole that
    # already has a name.
    if inherited and house:
        from src.orchestrator.seats import bind_holder, find_seat
        legacy_seat = await find_seat(actions.pool, house=house, handle=inherited)
        if legacy_seat:
            already_bound = await actions.pool.fetchval(
                "SELECT 1 FROM links l JOIN objects t ON t.id=l.to_id WHERE t.canonical=$1 "
                "AND l.type='holds' AND (l.valid_until IS NULL OR l.valid_until > now()) "
                "LIMIT 1", legacy_seat)
            if not already_bound:
                await bind_holder(actions, seat_id=legacy_seat, agent_id=heir, source=heir)
    await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        heir, ancestor_id)
    # ...and so does the READ STATE: the heir inherits the ancestor's recipient rows, or every
    # mint (i.e. every compaction) would redeliver the project's whole settled broadcast
    # history to the new mind. The heir literally remembers reading them — that memory is
    # exactly what survived the seam.
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, delivered_at, read_at, deliveries)"
        " SELECT message_id, $1, delivered_at, read_at, deliveries FROM message_recipients"
        " WHERE agent_id=$2 ON CONFLICT (message_id, agent_id) DO NOTHING", heir, ancestor_id)
    return heir, a


async def fold_existing_zero_turn_phantoms(actions: Actions) -> list[dict[str, Any]]:
    """RETROACTIVE CLEANUP (ruling d3531cd8, msg 1398: 'Fold existing zero-turn phantoms') —
    the going-forward fix (mint sites call _fold_zero_turn_ancestors before minting) does
    nothing for generations already minted before this fix landed, like the canonical repro
    itself (xxv, minted by /compact, superseded by /model before its first turn). Sweeps
    every ALREADY-SUPERSEDED, ALREADY-MINTED Agent (has succeeded_from AND succeeded_by, so
    a live descendant exists) that isn't already false_mint, folding each one exactly the
    live path would have. Safe to run repeatedly — an already-folded phantom carries
    false_mint and is excluded by construction. Returns what it folded, for the record."""
    # A GENEROUS pre-filter, deliberately: every minted (non-root) Agent, live head included
    # — correctness rests on _fold_zero_turn_ancestors's own agent_has_acted gate, not on
    # this query, so a live head with real acts (or an already-folded phantom, whose walk
    # halts at itself just as harmlessly) is a fast, safe no-op rather than something this
    # query must itself get exactly right (the value-comparison this would otherwise need —
    # 'is succeeded_by CURRENTLY non-empty' — is exactly the winning-row read the SQL
    # hygiene tripwire exists to keep out of a bare EXISTS).
    candidates = await actions.pool.fetch(
        "SELECT o.id, o.canonical FROM objects o "
        "WHERE o.type='Agent' AND o.status='active' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='minted_because')")
    now = datetime.now(UTC)
    folded: list[dict[str, Any]] = []
    for row in candidates:
        restored_id, restored_oid = await _fold_zero_turn_ancestors(
            actions, row["canonical"], row["id"], now)
        if restored_id != row["canonical"]:
            folded.append({"phantom": row["canonical"], "restored_to": restored_id})
    return folded


# NOTIFY-AT-SEAM (thread aeae9977, Ra's ask #1): "a compacting bodied worker's manager learns
# from the FLEET, not the human." Only a harness-reported context death fires this — the
# SILENT class nobody else is watching. model-succession and live-swap already surface on the
# membrane's DANGER map; reanimation-of-retired is a deliberate act, not an accident that
# strands a manager mid-conversation. KNOWN v1 GAP (Thoth's call, DM 1212): reanimation
# co-occurring with a REAL compaction is excluded too — when both fire, mint_because reads
# "reanimation-of-retired", never "compaction", so it never matches this whitelist. Left this
# way on purpose: it's rare, and widening the whitelist now would trade v1's whole value —
# precision on the silent class — for a case nobody's been bitten by yet. A successor who IS
# bitten by it finds the gap named here, not rediscovered.
_SEAM_NOTIFY_REASONS = {"compaction", "context-clear"}


async def _notify_seam_manager(
    actions: Actions, *, heir: str, mint_because: str, project: str | None,
) -> None:
    """A worker that just silently died and came back DMs its OWN manager — Ra's clean repro
    (aeae9977): a mail send-receipt refused the manager's DM to a fresh successor while the
    daemon held a live job the whole time, and the human had to notice and tell him. This is
    the fix: the successor reports itself, with the daemon's own reachability() evidence
    inline (Thoth's requirement — the manager gets a confirmation, not our say-so). Silent
    no-op when there's no seat or no manager of record — same 'nobody to confess to' shape
    Stage A's stop-hook confession already uses."""
    from src.orchestrator.mailbox import send_message
    from src.orchestrator.seats import held_seat, manager_of_seat, reachability

    bound = await held_seat(actions.pool, heir)
    if bound is None:
        return
    manager_seat = await manager_of_seat(actions.pool, bound["seat_id"])
    if manager_seat is None:
        return
    check = await reachability(actions.pool, heir)
    handle = bound["handle"] or heir
    body = (f"{handle} just {mint_because.replace('-', ' ')} — new generation {heir}. "
           f"{check['detail']}")
    await send_message(actions.pool, from_agent=heir, from_project=project,
                       to_agent=manager_seat, body=body, grade="fyi")


_SEAM_DEBOUNCE_SECS = 900
_DEBOUNCE_SRC = "seam-debounce"


async def agent_has_acted(
    actions: Actions, agent_id: str, *, exclude: list[uuid.UUID],
    settled_after: datetime | None,
) -> bool:
    """A MIND IS WITNESSED BY ITS ACTS (the debounce's law, b813e389): did this agent ever do
    anything beyond its own mint/registration bookkeeping? Acts = assertions on objects other
    than the excluded lineage pair, words sent, or mail SETTLED after the mint. NOT acts: the
    display-name stamps registration writes onto the repo and principal objects (a greeting's
    paperwork — the REGISTER path stamps those on every mount, and counting them made every
    register-minted heir read as a mind, so the cross-path debounce could never heal one)."""
    return bool(await actions.pool.fetchval(
        "SELECT EXISTS (SELECT 1 FROM assertions x JOIN objects o ON o.id=x.object_id "
        "         WHERE x.source_id=$1 AND NOT (x.object_id = ANY($2::uuid[])) "
        "           AND NOT (o.type IN ('SoftwareProject','Person') AND x.name='name')) "
        "  OR EXISTS (SELECT 1 FROM fleet_messages WHERE from_agent=$1) "
        "  OR EXISTS (SELECT 1 FROM message_recipients "
        "         WHERE agent_id=$1 AND read_at IS NOT NULL "
        "           AND ($3::timestamptz IS NULL OR read_at > $3))",
        agent_id, exclude, settled_after))


async def _debounce_roundtrip(
    actions: Actions, *, agent_id: str, observed: str, now: datetime,
    job_dir: str | None = None,
) -> dict[str, Any] | None:
    """THE SEAM DEBOUNCE (Soundwave VII's wave-3 grievance, b813e389): the operator toggling
    /model there-and-back within a minute minted a generation — roman-numeral churn for
    settings churn dilutes what the numeral MEANS (ruling a882b334: the numeral tracks the
    MIND). The distinction that keeps both truths: a mind is witnessed by its ACTS. When the
    model returns to the seam's left side within the window and the transient heir asserted
    nothing beyond its own mint stamps, sent nothing, and settled nothing — no mind ever
    existed; the mint heals as false (event-sourced, compensating, its record stays) and the
    ancestor takes its seat back, estate included. One witnessed act, and the heir stands:
    a real mind passed through, however briefly. Returns the heal dict, or None (mint on).

    SHARED BY BOTH MINT PATHS (thread a3d49d91): it originally lived only in the chrome
    heartbeat and only healed heads minted 'live-swap' — so a round-trip whose return leg was
    witnessed by a MOUNT (register_agent) could never heal, and the two observers ping-ponged
    generations off each other's stamps (TJMAX VI→X, five mints in six minutes). `agent_id`
    must be the LINEAGE HEAD; `job_dir` re-points that mount row when the caller has one,
    else any row naming the healed heir follows the restored ancestor."""
    cur = agent_id
    cur_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'", cur)
    if cur_oid is None:
        return None
    meta = {r["name"]: (r["v"], r["at"]) for r in await actions.pool.fetch(
        "SELECT DISTINCT ON (name) name, value #>> '{}' AS v, observed_at AS at "
        "FROM current_assertions WHERE object_id=$1 "
        "AND name IN ('succeeded_from','minted_because','model_succession') "
        "ORDER BY name, confidence DESC, observed_at DESC", cur_oid)}
    # both MODEL-seam mints heal; a compaction/clear/reanimation mint is a context death,
    # not model flapping — there is no 'left side' to return to
    if meta.get("minted_because", (None, None))[0] not in ("live-swap", "model-succession"):
        return None
    ancestor, minted_at = meta.get("succeeded_from", (None, None))
    seam = meta.get("model_succession", ("", None))[0] or ""
    if (not ancestor or minted_at is None
            or (now - minted_at).total_seconds() > _SEAM_DEBOUNCE_SECS):
        return None
    left = normalize_model(seam.split("→")[0].strip()) if "→" in seam else None
    if left is None or left != observed:
        return None  # not a round-trip — a third model is a real third mind
    ancestor_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", ancestor)
    if ancestor_oid is None:
        return None
    # acts = assertions beyond the lineage bookkeeping pair, words sent, or mail SETTLED
    # after the mint (a lease/delivery is passive perception, never an act)
    if await agent_has_acted(actions, cur, exclude=[cur_oid, ancestor_oid],
                             settled_after=minted_at):
        return None
    do = EvidenceClass.DIRECT_OBSERVATION
    conf = confidence_for(do)
    for k, v in (("false_mint", True), ("retired", True), ("retired_by", _DEBOUNCE_SRC),
                 ("false_mint_because",
                  "model round-trip within the debounce window, no witnessed act — "
                  "settings churn, not a death (Soundwave's grievance, b813e389)")):
        await actions.assert_property(cur_oid, k, v, _DEBOUNCE_SRC, now, conf,
                                      evidence_class=do.value)
    # unwind the head-walk (the old pointer stays in history — compensating, never deleted)
    await actions.assert_property(ancestor_oid, "succeeded_by", "", _DEBOUNCE_SRC, now, conf,
                                  evidence_class=do.value)
    # the estate returns: unread DMs re-address to the restored mind; read state needs no
    # unwind (the heir's copied rows are inert once the heir is retired)
    await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        ancestor, cur)
    # ...and so does THE BINDING (the seat world's estate, 5cef856b): mint_heir moved the
    # holds link to the transient heir; a heal that leaves it there strands every
    # seat-addressed DM on a false mint the read predicate still honors. The binding
    # follows the head — and after a heal, the head IS the restored ancestor.
    from src.orchestrator.seats import follow_binding
    await follow_binding(actions, ancestor_oid=cur_oid, heir=ancestor,
                         heir_oid=ancestor_oid, now=now)
    if job_dir is not None:  # the heartbeat's caller holds the row — bump its pulse too
        await actions.pool.execute(
            "UPDATE agent_mounts SET agent_id=$2, model=$3, last_seen=now() WHERE job_dir=$1",
            job_dir, ancestor, observed)
    else:  # the register path: any row naming the healed heir follows the restored mind
        await actions.pool.execute(
            "UPDATE agent_mounts SET agent_id=$2, model=$3 WHERE agent_id=$1",
            cur, ancestor, observed)
    return {"healed": cur, "restored": ancestor,
            "seam": f"{seam} → {observed} (round-trip within "
                    f"{_SEAM_DEBOUNCE_SECS // 60}m, no act — debounced, not a death)"}


async def _already_reached(actions: Actions, *, agent_id: str, observed: str) -> bool:
    """Did this lineage HEAD already record a swap landing on `observed`? (idempotency on
    /succession, thread 8dc9940c — Thoth's own live repro, three 'live-swap' mints for one
    real fable→opus transition.) The comparison live_succession runs the seam against —
    agent_mounts.model — is a MUTABLE row that can drift back to a stale value after the
    real swap already completed (mount()'s own re-derivation resets it; the deeper cause is
    banked as a separate question). The head's own `source_model` is equally mutable, reset
    by the same path. `model_succession` is not: mint_heir stamps it EXACTLY ONCE, at the
    mint that recorded the swap, and nothing ever touches it again — the one write-once
    witness immune to the drift. If the head's own recorded transition already landed on
    `observed`, a fresh 'stored != observed' reading is re-discovering a COMPLETED swap, not
    witnessing a new one — minting again would just be the duplicate ruling 95dff46f warned
    against, one lineage, three numerals, one transition. A genuinely NEW target (observed
    differs from what's already recorded) returns False and mints exactly as before."""
    seam = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND o.status='active' AND a.name='model_succession' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent_id)
    if not seam or "→" not in seam:
        return False
    right = seam.split("→", 1)[1].strip()
    target = normalize_model(right.split("[", 1)[0].strip())
    return target is not None and target == observed


async def live_succession(
    actions: Actions, *, session_id: str, observed_model: str,
) -> dict[str, Any]:
    """A mid-session model change, sensed by the chrome heartbeat (ruling a882b334): the mind
    changed under a LIVE tab, so the seat passes now — mint the heir, move the durable mount
    row, and every per-render read (statusline, stop hook, digest) resolves to the new mind
    from the next glance. Idempotent: an unchanged model or an unknown mount is a no-op; a row
    with no stored model gets a first stamp, not a funeral (you can only die if you lived)."""
    sid = (session_id or "").strip().lower()
    observed = normalize_model(observed_model)
    if len(sid) < 8 or not observed:
        return {"unchanged": True, "reason": "no anchor"}
    # the ONE session→row lookup (mounts.find_session_row, task #33) — an inline copy
    # here once meant a swap in a re-anchored window went unwitnessed
    from src.orchestrator.mounts import find_session_row
    row = await find_session_row(actions.pool, sid)
    if row is None:
        return {"unchanged": True, "reason": "no mount"}
    if normalize_model(row["model"]) == observed:
        if row["model"] != observed:  # converge a bracket-stamped row to the canonical form
            await actions.pool.execute(
                "UPDATE agent_mounts SET model=$2 WHERE job_dir=$1", row["job_dir"], observed)
        return {"unchanged": True}
    async with mint_lock(actions.pool, _generation(row["agent_id"])[0]):
        # RE-READ INSIDE THE LOCK: two concurrent heartbeats both read the pre-swap row and
        # both minted — Soundwave VI and VII, identical seam strings, one second apart
        # (2026-07-14). The loser now waits, re-reads, sees the winner's write, no-ops.
        row = await find_session_row(actions.pool, sid)
        if row is None:
            return {"unchanged": True, "reason": "no mount"}
        old = normalize_model(row["model"])
        if old == observed:
            if row["model"] != observed:
                await actions.pool.execute(
                    "UPDATE agent_mounts SET model=$2 WHERE job_dir=$1",
                    row["job_dir"], observed)
            return {"unchanged": True}
        # THE NULL-SEAM GATE (thread 065c374e, mirroring forks.py's lesson here as
        # defense-in-depth): a row with no stored model was never OBSERVED, not observed-as-
        # something-else — it can never "disagree with" the first real reading, so this is a
        # first stamp, never a seam to mint against.
        if old is None:
            await actions.pool.execute(
                "UPDATE agent_mounts SET model=$2 WHERE job_dir=$1", row["job_dir"], observed)
            return {"unchanged": True, "reason": "first stamp"}
        now = datetime.now(UTC)
        # seams run on the lineage HEAD — and the debounce judges the head, not the row,
        # which may lag its own succession
        head = await lineage_head(actions.pool, row["agent_id"])
        # a there-and-back /model toggle with no act between heals instead of minting again
        healed = await _debounce_roundtrip(actions, agent_id=head, observed=observed,
                                           now=now, job_dir=row["job_dir"])
        if healed is not None:
            return healed
        # IDEMPOTENCY (thread 8dc9940c): the head already recorded reaching THIS exact
        # model once — a fresh disagreement against the (mutable, driftable) stored row is
        # the same completed swap resurfacing, not a new one. Repair the drifted stamps in
        # place; mint nothing.
        if await _already_reached(actions, agent_id=head, observed=observed):
            do = EvidenceClass.DIRECT_OBSERVATION
            head_oid = await actions.create_or_find_object("Agent", head, head)
            await actions.assert_property(head_oid, "source_model", observed, head, now,
                                          confidence_for(do), evidence_class=do.value)
            await actions.pool.execute(
                "UPDATE agent_mounts SET model=$2, last_seen=now() WHERE job_dir=$1",
                row["job_dir"], observed)
            return {"unchanged": True,
                   "reason": f"idempotent — {head} already recorded reaching {observed}; "
                             "repaired the drifted stored model, minted nothing"}
        # whose hand moved the model? A /model on THIS session's own transcript makes the seam
        # the OPERATOR's deliberate act — the mint still happens (a death is a death, ruling
        # a882b334) but the seam string carries the hand, so no downstream surface preaches.
        deliberate = False
        try:
            main = locate_current_transcript(
                Path.home() / ".claude/projects", row["job_dir"], anchored_only=True)
            if main is not None:
                _cur, _hist, deliberate = model_of_transcript(main)
        except OSError:
            deliberate = False
        ancestor_oid = await actions.create_or_find_object("Agent", head, head)
        # SUCCESSION FOLLOWS TURNS (ruling d3531cd8): fold any zero-turn phantom off the
        # front of the chain BEFORE minting on top of it — head/ancestor_oid below name
        # whoever this heir actually succeeds, not a compaction-minted phantom that never
        # took a turn.
        head, ancestor_oid = await _fold_zero_turn_ancestors(actions, head, ancestor_oid, now)
        seam = f"{old} → {observed}" + (" [operator /model]" if deliberate else "")
        heir, heir_oid = await mint_heir(actions, head, ancestor_oid, because="live-swap",
                                         succession=seam, now=now,
                                         minting_door=row["job_dir"])
        # the heartbeat's model is the harness's own word about a session it is rendering — as
        # anchored as a job_dir transcript read, and the baseline the NEXT seam check runs
        # against (without it, a later re-mount would see no anchored model on the heir and
        # stay quiet).
        do = EvidenceClass.DIRECT_OBSERVATION
        await actions.assert_property(heir_oid, "source_model", observed, heir, now,
                                      confidence_for(do), evidence_class=do.value)
        if row["project"]:
            await actions.assert_property(heir_oid, "project", row["project"], heir, now, _CONF,
                                          evidence_class=_EC)
        sid_prop = _job_id(row["job_dir"]) or sid[:8]
        await actions.assert_property(heir_oid, "session", sid_prop, heir, now, _CONF,
                                      evidence_class=_EC)
        await actions.pool.execute(
            "UPDATE agent_mounts SET agent_id=$2, model=$3, last_seen=now() WHERE job_dir=$1",
            row["job_dir"], heir, observed)
        handle = await actions.pool.fetchval(
            "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='handle'",
            heir_oid)
        return {"minted": heir, "from": head, "succession": seam,
                "seat": seat_label(heir, handle)}


async def _winning_retired(actions: Actions, agent: uuid.UUID) -> bool:
    """True if this Agent carries a winning retired=true — a deliberate close. Read off the
    projected current_assertions (highest confidence, then most recent), same predicate the
    trigger's reanimation-guard uses, so mount and wake agree on 'is this identity closed'."""
    v = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions "
        "WHERE object_id=$1 AND name='retired' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", agent)
    return bool(v == "true")


async def register_agent(
    actions: Actions, identity: AgentIdentity, *, actor: str, expected_model: str | None = None,
    mint_reason: str | None = None,
) -> uuid.UUID:
    """Mint (idempotently) the Agent object + its org-chart links. The agent attributes
    its OWN registration (`source = agent:<session>`), SELF_DECLARED. Re-mount is a no-op
    (find-or-create + the kernel's byte-dup assertion skip absorb it). `expected_model` (the
    operator's standing choice) turns on the swap-detector: the intent is stamped, and a silent
    demotion away from it is recorded as a first-class OBSERVED event on the Agent.

    THE SUCCESSION SEAM (bug #51, a sibling project): session-keyed identity means a retire+compact+
    swap hands a DEAD agent's id to a fresh context — a different model then writes AS it, and
    the transcript-level swap-detector is blind when the new transcript never ran the old model.
    So registration also compares the fresh ANCHORED observation against the graph's last
    anchored source_model: ANY disagreement is a succession seam under the mind ruling
    (a882b334) — even one the transcript witnessed. The old exemption for witnessed transitions
    ("same context, different seam") encoded tenure semantics: the operator overruled it — the
    numeral tracks WHICH MIND, and a mind is one contiguous run of one model, so a witnessed
    swap is a death like any other (the warm-swap `model_swapped` stamp still lands too — both
    records are true). `mint_reason` forces a mint for a context-death the harness reported
    with no model change at all (compaction, /clear): the weights survive but the memory the
    operator was talking to does not."""
    now = datetime.now(UTC)
    obs: str | None = None
    # THE MINT LOCK (thread a3d49d91): phases 0–1 read-then-write the succession chain; two
    # concurrent registrations (or a registration racing the heartbeat) must serialize per
    # lineage, or the loser's head-walk finds the winner's mint and stacks a generation on it.
    async with mint_lock(actions.pool, _generation(identity.agent_id)[0]):
        # PHASE 0 — LINEAGE (ruling be292762): a session-keyed resolve lands on the BASE id;
        # walk to the lineage HEAD first — the head is who this name is now. Seam checks run
        # against the head.
        head = await lineage_head(actions.pool, identity.agent_id)
        if head != identity.agent_id:
            identity.agent_id = head
        src = identity.agent_id
        a = await actions.create_or_find_object("Agent", identity.agent_id, src)

        # PHASE 1 — SEAM DETECTION → MINT (the operator's ruling: the heir gets its OWN name).
        mint_because: str | None = None
        if await _winning_retired(actions, a):
            # wearing a RETIRED face (bug #51 follow-up): under the mint ruling the retiree is
            # never re-worn — the arriving context is an heir and gets minted below. The
            # retirement stands.
            mint_because = "reanimation-of-retired"
        anchored = bool(identity.model) and identity.model_method == "job_dir"
        if anchored:
            # the succession seam: read the baseline BEFORE the new observation supersedes it.
            # No witnessed-transition exemption (ruling a882b334): oscillation mints every
            # time — the returning model is a THIRD mind, not the first one back. NORMALIZED
            # comparison: a bracketed display variant of the same weights is the same mind,
            # never a seam.
            prior_raw, prior_at = await _last_anchored_stamp(actions, a)
            prior = normalize_model(prior_raw)
            obs = normalize_model(identity.model)
            # THE NULL-SEAM GATE (thread 065c374e, defense-in-depth for forks.py's lesson: a
            # NULL/'unknown' prior is the ABSENCE of an observation, never a value — WE HAVE
            # NOT LOOKED YET is not "the mind was someone else". A fresh reading can't disagree
            # with a null, so `prior is not None` must gate every comparison below it.
            if prior is not None and prior != obs:
                # THE DATING GATE (thread a3d49d91): the transcript tail LAGS a /model — no
                # assistant turn has run on the new model yet — so an observation not FRESHER
                # than the stamp it disagrees with is an old newspaper arguing with today's,
                # never a seam. (TJMAX VIII/IX: opposite seams, four seconds apart, minted
                # off each other's stale reads.) An unstamped observation keeps the old
                # behavior: the gate only ever SUPPRESSES a mint it can prove stale.
                stale = (identity.model_observed_at is not None and prior_at is not None
                         and identity.model_observed_at <= prior_at)
                if not stale:
                    identity.model_succession = f"{prior} → {obs}"
                    mint_because = mint_because or "model-succession"
        if mint_reason:
            # a harness-reported context death (compaction, /clear) with no model seam of its
            # own
            mint_because = mint_because or mint_reason
        if mint_because == "model-succession" and not mint_reason and obs is not None:
            # a model seam ALONE may be settings flapping: try the heal before minting — the
            # debounce must work whichever observer witnesses the return leg (it used to live
            # only in the heartbeat, so a mount seeing the round-trip minted a phantom).
            healed = await _debounce_roundtrip(actions, agent_id=identity.agent_id,
                                               observed=obs, now=now)
            if healed is not None:
                identity.agent_id = str(healed["restored"])
                src = identity.agent_id
                a = await actions.create_or_find_object("Agent", identity.agent_id, src)
                identity.model_succession = None
                mint_because = None
        if mint_because:
            # SUCCESSION FOLLOWS TURNS (ruling d3531cd8): fold any zero-turn phantom off
            # the front of the chain BEFORE minting — succeeded_from must land on whoever
            # this heir actually succeeds, not a phantom that never took a turn (the exact
            # gap that left orient()'s inheritance block blind on a double-mint, e749036e).
            identity.agent_id, a = await _fold_zero_turn_ancestors(
                actions, identity.agent_id, a, now)
            identity.succeeded_from = identity.agent_id
            heir, a = await mint_heir(actions, identity.agent_id, a, because=mint_because,
                                      succession=identity.model_succession, now=now,
                                      minting_door=identity.session,
                                      upcoming_project=identity.project)
            identity.agent_id = heir
            src = heir
    if mint_because is not None and mint_because == mint_reason and (
        mint_reason in _SEAM_NOTIFY_REASONS
    ):
        try:
            await _notify_seam_manager(actions, heir=src, mint_because=mint_because,
                                       project=identity.project)
        except Exception as exc:  # noqa: BLE001 — Ra's bug (aeae9977) was SILENCE; a
                                   # notify failure must never be the thing that blocks a
                                   # mount, but swallowing it WITHOUT A TRACE would just
                                   # relocate the same silence one layer down (Thoth's
                                   # review, DM 1216) — fail open, never fail quiet
            logger.warning("notify-at-seam failed for heir %s (%s): %r",
                           src, mint_because, exc)
    label = f"{identity.model or 'claude'} in {identity.project or '?'}"
    await actions.assert_property(a, "name", label, src, now, _CONF, evidence_class=_EC)
    await actions.assert_property(a, "session", identity.session, src, now, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(a, "identity_resolved", identity.resolved, src, now, _CONF,
                                  evidence_class=_EC)
    if identity.model:
        ec = _MODEL_EC.get(identity.model_method or "", EvidenceClass.CO_OCCURRENCE)
        # dated by the EVENT (the transcript record that carried the model), never by the
        # bookkeeping — so the next seam check compares clocks honestly: a fresher heartbeat
        # stamp beats this one, an older tail read loses to it.
        await actions.assert_property(a, "source_model", identity.model, src,
                                      identity.model_observed_at or now,
                                      confidence_for(ec), evidence_class=ec.value)
    if identity.model_divergent and identity.model_declared:
        # the agent self-reported a model that DISAGREES with the harness (ruling 17516660): keep
        # its word as the weak signal it is — the mismatch with source_model (observed) IS the flag.
        sr = EvidenceClass.CO_OCCURRENCE
        await actions.assert_property(a, "source_model_declared", identity.model_declared, src,
                                      now, confidence_for(sr), evidence_class=sr.value)
    if expected_model:
        # the swap-detector (ruling f2ae6346): stamp the INTENT, and when the observed model
        # diverges from it — the fable harness's silent danger-demotion — record the swap as a
        # first-class OBSERVED event (not the agent's self-report; it can't feel its own swap).
        # gate the swap on a job_dir ANCHOR: a cwd/self-report model may be a neighbor's, and a
        # divergence asserted off it is the cry-wolf — the true positive is the anchored read.
        # The repo's OWN declared intent (.osiris model=) outranks the box default: a fleet of
        # onboarded repos does not all run fable, and the operator's choice is never a sin.
        expected_model = read_project_model(identity.cwd) or expected_model
        verdict = classify_swap(identity.model_history, identity.model, expected=expected_model,
                                anchored=identity.model_method == "job_dir",
                                deliberate=identity.model_deliberate)
        await actions.assert_property(a, "model_intent", expected_model, src, now, _CONF,
                                      evidence_class=_EC)
        if verdict.swapped and not verdict.deliberate:
            # RE-SCOPED (task #146, operator's own words: "a rug pull ... vs a direct /model
            # swap on my part is different"): `model_swapped` is the EXACT property the
            # digest's danger map reads (sessions.py's own miner docstring) — stamping it for
            # a WITNESSED, deliberate /model is a false positive on that map, indistinguishable
            # from the harness's silent danger-demotion this property exists to catch. The
            # confession is for the harness changing the model WITHOUT the operator; an
            # operator /model is recorded durably below instead (intended_model + the pin),
            # never as a danger sighting.
            do = EvidenceClass.DIRECT_OBSERVATION
            await actions.assert_property(a, "model_swapped", swap_marker(verdict), src, now,
                                          confidence_for(do), evidence_class=do.value)
        if verdict.deliberate and verdict.to_model:
            # THE STANDING-CHOICE WRITE SIDE (operator ruling e0e0955d, confirming his own
            # 1aca1fcc from 2026-07-19): the operator's own /model command on the record IS
            # the operator re-pinning this seat's standing choice — auto-stamp intended_model
            # so the choice persists across successions and relaunches with no manual
            # re-pinning. The READ side already exists (mint_seat's own pin, launch()'s
            # precedence since 70ae3c3); this is the one write it was missing. Gated on
            # `deliberate` specifically (never `swapped` alone — swaps.py's own law: only a
            # WITNESSED /model transition sets it, never a harness rug-pull or a cold
            # divergence guessed from the intent alone).
            from src.orchestrator.seats import held_seat
            seat = await held_seat(actions.pool, identity.agent_id)
            if seat:
                soid = await actions.create_or_find_object("Seat", seat["seat_id"], src)
                await actions.assert_property(soid, "intended_model", verdict.to_model, src,
                                              now, _CONF, evidence_class=_EC)
                # THE PIN BECOMES A CACHE, NOT A COMPETING CLAIM (task #146): the graph stamp
                # above is durable but invisible to `_expected_model`'s FIRST-checked source
                # (the .osiris file itself) and to a fresh `osiris launch` on a box that never
                # talks to this graph. Writing the file closes both gaps in one act — no other
                # reader needs to change, since read_project_model/_expected_model already
                # check the file before anything else.
                if seat.get("handle"):
                    await write_model_pin(str(seat["handle"]), verdict.to_model)
    # RULE 1 OF de3dfc18 (task #144): "where this lineage's work actually landed" — ported
    # from project_identity.py's own _write_attribution (task #110), the SAME query, reused
    # rather than re-derived. Bases = this agent's OWN lineage only (not a seat's holder
    # history — resolve_identity/register_agent run before any seat necessarily exists, so
    # the agent's own generation-stripped id is the one lineage key guaranteed on hand).
    #
    # THE ACCEPTANCE CONDITION (Thoth's own words, msg 3854): "if it picks, it is wrong,
    # however good the pick." This NEVER overwrites `identity.project` — it reports
    # agreement/disagreement HONESTLY and stops. A later, separately-scoped build decides
    # whether/when rule 1 gets to WIN a disagreement; this lane only makes the disagreement
    # visible, which nothing before it could do at all.
    #
    # DEGRADES, NEVER BLOCKS (577988ed — this sits on the mount path every session in the
    # fleet traverses): a failed query here must never be the reason a mount fails. None/0/
    # None (the dataclass defaults) is an honest "could not determine", not a wrong answer.
    try:
        from src.orchestrator.project_identity import _write_attribution
        wa = await _write_attribution(actions.pool, [_generation(identity.agent_id)[0]])
    except Exception as exc:  # noqa: BLE001 — a DB hiccup on a diagnostic signal must
                               # never be the thing that blocks a mount (577988ed)
        logger.warning("write-attribution check failed for %s: %r", identity.agent_id, exc)
        wa = None
    if wa is not None:
        identity.write_attribution_top = wa["top"]
        identity.write_attribution_total = wa["total"]
        if wa["total"] == 0:
            identity.write_attribution_agreement = "no-signal"
        elif wa["top"] == identity.project:
            identity.write_attribution_agreement = "confirms"
        else:
            identity.write_attribution_agreement = "disagrees"
            # a DISAGREEMENT is the actionable case — durable, so a later audit (or a human
            # skimming dossier()) can see it without having caught the live mount() banner.
            # DERIVED evidence (an inference from write history, not a declaration): weaker
            # than the SELF_DECLARED properties around it, on purpose.
            do_ec = EvidenceClass.DERIVED
            await actions.assert_property(
                a, "write_attribution_disagreement",
                f"lineage writes mostly to {wa['top']!r} ({wa['breakdown'].get(wa['top'], 0)}"
                f"/{wa['total']}) but this session resolved {identity.project!r}",
                src, now, confidence_for(do_ec), evidence_class=do_ec.value)
    if identity.project:
        await actions.assert_property(a, "project", identity.project, src, now, _CONF,
                                      evidence_class=_EC)
        proj = await _resolve_or_mint_project(actions, identity.project, src)
        # THE PROJECT-NAME CLOBBER (task #137/#152, Thoth DM 3801): this used to reassert
        # `name` from the caller's own pin/identity.project UNCONDITIONALLY, at the SAME
        # self_declared confidence a deliberate rename_project/correct_project_name write
        # uses — current_assertions' tie-break (confidence DESC, observed_at DESC) then
        # falls through to pure recency, so any later, uninformed mount silently overturns
        # an earlier, reasoned rename. LIVE, MEASURED: repo:xxit's declared name
        # "handlingtheloop" (decision 8766acd7, 2026-07-31/08-02) was reverted to "xxit"
        # by five ordinary metron/deckard mounts between 2026-08-07 and 2026-08-08 —
        # the byebyte disease (9550e980) recurring through a far more common trigger than
        # disk-census: this line, on every mount of a seat with a stale pin. FIX: only
        # write at full confidence when there is no existing declared name yet, or the
        # difference is case/whitespace-only (the already-delegated-safe exception,
        # ruling 1db1ff41 / decision 8cf283f4). A genuine difference is never silently
        # dropped — still recorded, so nothing is hidden from history or from
        # project_identity_evidence's own audit — but at DERIVED-tier confidence, so a
        # routine, uninformed mount can never outrank a declared rename on recency alone.
        existing_name = await actions.pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
            "AND name='name' ORDER BY confidence DESC, observed_at DESC LIMIT 1", proj)
        if (existing_name is None
                or existing_name.strip().casefold() == identity.project.strip().casefold()):
            await actions.assert_property(proj, "name", identity.project, src, now, _CONF,
                                          evidence_class=_EC)
        else:
            do = EvidenceClass.DERIVED
            await actions.assert_property(proj, "name", identity.project, src, now,
                                          confidence_for(do), evidence_class=do.value)
        await _link_once(actions, a, proj, "works_in", src, now)
    if identity.cwd:  # the repo path — lets the trigger-hook resolve a project → where to wake
        await actions.assert_property(a, "cwd", identity.cwd, src, now, _CONF, evidence_class=_EC)
    principal = await actions.create_or_find_object("Person", f"principal:{actor}", src)
    await actions.assert_property(principal, "name", actor, src, now, _CONF, evidence_class=_EC)
    await _link_once(actions, a, principal, "acts_for", src, now)
    return a


async def seat_bearings(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """WHO AM I, AND WHOSE JOB IS VACANT HERE? (Ra V, a-sibling, msg 384 — the gap that made
    the whole ruling hollow.)

    The HOUSE/SEAT/HOLDER ruling stamped his seat in the graph, and orient() went on answering
    `"you": "agent:c7ef52a9-iii"`. His words: "The refusal is fixed; the DISCOVERY isn't. He will
    not be refused as a stranger anymore. He will simply never learn the family name exists." A
    fresh mind reads the briefing and NOTHING ELSE — so an inheritance nobody is told about is not
    an inheritance. It protects a name the next holder will never reach for.

    So the briefing now says it: your seat if you hold one; and if you are anonymous, the seats of
    your house that are standing empty, with the verb that takes them."""
    seat = await pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='handle' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='seat_generation' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS gen, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='project' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS house "
        "FROM objects o WHERE o.canonical=$1", agent_id)
    # THE BINDING IS PART OF WHO YOU ARE (Phase B3, 5cef856b): a mind that actively HOLDS a
    # Seat object is told so — whether or not it ever claim_named itself in the assertion
    # world. An attached-at-birth mind that reads its own orient() and sees nothing was the
    # discovery gap all over again (Ra V's grievance, one layer down).
    from src.orchestrator.seats import held_seat
    bound = await held_seat(pool, agent_id)
    binding = {"seat_binding": bound} if bound else {}
    # RULING 1 (decision 1db1ff41): a SEATED mind's house is DERIVED — held_seat already
    # walked the managed_by chain to compute `binding`'s own seat_binding.house; this
    # function used to ignore that and read the Agent's own raw `project` stamp instead (the
    # same duplicate house_of() reads independently), the exact bypass orient() shipped
    # through. An UNSEATED mind has no seat to walk, so the raw stamp remains its only
    # signal — that branch is unchanged.
    house = bound["house"] if bound and bound.get("house") else (seat["house"] if seat else None)
    if seat and seat["handle"]:
        gen = int(seat["gen"]) if seat["gen"] else None
        return {"seat": seat_label(agent_id, seat["handle"], gen), "house": house,
                **binding}

    if not house:
        return binding
    # anonymous: what jobs does this house have, and is anyone sitting in them?
    names = [r["handle"] for r in await pool.fetch(
        "SELECT DISTINCT (SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle "
        "FROM objects o WHERE o.type='Agent' "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='project' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '') = $1", house)]
    vacant = []
    for n in (x for x in names if x):
        holders = await seat_holders(pool, house, n)
        live = await pool.fetchval(
            "SELECT count(*) FROM agent_mounts WHERE agent_id = ANY($1::text[]) "
            "AND last_seen > now() - interval '15 minutes'", holders)
        if not live:
            vacant.append({"seat": n, "holders": len(holders),
                           "last_held_by": holders[-1] if holders else None})
    if not vacant:
        return {"house": house, **binding}
    return {"house": house, "vacant_seats": vacant, **binding,
            "note": f"you are anonymous in the house of {house}. These seats are STANDING EMPTY — "
                    "claim_name('<seat>') INHERITS one (you become its next holder; the previous "
                    "holders' work stays theirs). A seat a LIVE mind holds is not vacant and will "
                    "be refused: two minds in one house do two jobs."}
