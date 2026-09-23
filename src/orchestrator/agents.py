"""Actor identity: each connecting agent is registered in the graph as its own actor.

The persistent MCP server is one process the whole fleet writes through, so without this
every agent's writes collapse into the single `session` source, an undifferentiated mush.
This resolves each connecting agent into a distinct actor and registers it in the graph:

  * project, from the agent's cwd (it always knows where it's working);
  * model, observed off the agent's own session record through the transcript store (the
    source-model provenance, authoritative from the harness, not the system prompt),
    anchored on its job dir;
  * session, the job/session id, the stable handle.

An `Agent` object (canonical `agent:<session>`) is minted with those, linked `works_in` its
project and `acts_for` the principal, so the graph contains a record of who is doing the work.
Every write that agent then makes is attributed to `agent:<session>` instead of `session`,
which (a) makes provenance real: which instance, which model, decided what, and (b) keeps the
miner's ownership boundary intact (an agent source is never `session-miner`, so the backfill
miner never touches deliberate work).
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
from src.orchestrator.offices import _default_office_root, is_bare_office_root
from src.orchestrator.swaps import classify_swap, swap_marker
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.agents")

_EC = EvidenceClass.SELF_DECLARED.value
_CONF = confidence_for(EvidenceClass.SELF_DECLARED)


@dataclass
class AgentIdentity:
    """Who an agent is. `agent_id` is the source string its writes are attributed to."""

    agent_id: str  # "agent:<session>", the provenance source
    session: str
    project: str | None
    model: str | None
    cwd: str | None
    # How the model was resolved: grades the source_model assertion (job_dir probe = observation,
    # cwd = a weaker guess, self_report = the agent's own word). None when the model is unknown.
    model_method: str | None = None
    # Divergence flag: the agent's self-reported model (if it passed one), and whether it
    # disagrees with the harness observation. A self-report that lies is flagged.
    model_declared: str | None = None
    model_divergent: bool = False
    # The distinct models across this session's transcript, in first-seen order: the swap
    # history (more than one entry means a within-session demotion). Feeds the swap detector.
    model_history: tuple[str, ...] = ()
    # Whether an explicit /model command appears on this transcript's record: any within-session
    # model swap that follows one was chosen, not suffered.
    model_deliberate: bool = False
    # When the anchored model observation was witnessed: the timestamp of the transcript
    # record that carried it (None when unanchored, or the record was unstamped). The seam
    # gate compares clocks with this: the tail lags a /model command until the next assistant
    # turn, so an observation not fresher than the stamp it disagrees with is a stale read,
    # never a real model swap. source_model is stamped at this moment too, so the ledger is
    # dated by the event, not by the bookkeeping.
    model_observed_at: datetime | None = None
    # False when identity fell back to a best-effort id (no session/job-id/transcript anchor).
    # The fallback is distinct per session rather than a shared sink, so distinct actors can't
    # merge; the flag lets the fleet digest surface an unresolved onboarding.
    resolved: bool = True
    # Set by register_agent (the one out-param): "<prior> -> <observed>" when this registration
    # crossed a succession seam, i.e. a fresh context inheriting an identity another model wrote
    # under. None when no seam fired. mount() reads it to report the seam.
    model_succession: str | None = None
    # Set by register_agent: True when this mount re-attached an identity that was already marked
    # retired=true. The trigger already refuses to reanimate a retired identity (resume, not
    # mint), but a plain mount from the same session UUID would silently un-retire the name.
    # register_agent now stamps the reanimation as a first-class observed event and mount()
    # reports it, instead of letting it happen silently.
    reanimated: bool = False
    # Set by register_agent: this context was minted a new lineage-linked id
    # (agent:<base>-ii...) because it arrived across a detected seam or wore a retired face.
    # Holds the ancestor's canonical id; mount() reports the minting to the heir.
    succeeded_from: str | None = None
    # Could not read a `.osiris` declaration that exists: the file was found but failed to
    # parse/read, which is a different real state from "no declaration" (which stays the
    # legitimate unpinned None, unchanged). Set only when `_read_osiris_key`'s climb actually
    # hit a broken file for the `project` key; `project` above still falls back to the basename
    # guess exactly as before. None/None when nothing broke: the common case, never populated
    # speculatively.
    project_pin_error: str | None = None
    # The path does double duty: set alongside `project_pin_error` for a broken file, or alone
    # (error=None) for a valid `.osiris` that simply never declares `project` (correct TOML
    # answering a different question). `project_pin_banner` tells these apart by checking
    # `project_pin_error` first; a caller that only wants "is there a path to point at" can use
    # this either way.
    project_pin_path: str | None = None
    # True only when no `.osiris` exists anywhere in the climb at all: the third leg of a
    # three-way split (no pin / unparseable pin / parseable pin missing the key). Never set for
    # the bare seat-office root (a deliberate carve-out), when there is no cwd to climb from at
    # all, or when `cwd` itself doesn't exist (see `project_pin_cwd_missing` below, a different,
    # disjoint state), those stay silent by design, not "missing".
    project_pin_missing: bool = False
    # `cwd` itself does not exist on disk: set only when `_read_osiris_key`'s leaf check fails
    # before any climb even starts. Deliberately disjoint from `project_pin_missing`: a deleted
    # office and an unpinned-but-real office are opposite situations (one wants the graph's
    # stale belief cleaned up, the other wants a pin written), and folding them into one flag
    # is the exact bug this avoids. `project` still falls back to a basename guess either way
    # (unchanged); this only makes the reported reason honest.
    project_pin_cwd_missing: bool = False
    # Set by register_agent: the majority in_repo target across this agent's own lineage
    # ("where this lineage's work actually landed"), reported honestly, never used to overwrite
    # `project` above. `write_attribution_agreement` is one of "no-signal" (this lineage has
    # never filed an in_repo edge anywhere) / "confirms" (the majority target matches
    # `project`) / "disagrees" (it doesn't); never a fourth, silent "picked its own answer"
    # state; mount() reports "disagrees" but never acts on it. None/0/None when the DB check
    # itself couldn't run (a degrade, never a block).
    write_attribution_agreement: str | None = None
    write_attribution_top: str | None = None
    write_attribution_total: int = 0


# Roman generations for successor ids (e.g. agent:a8c15486-ii). The alphabet is deliberately
# restricted to {i, v, x}, none of which are hex digits, so a full-UUID canonical like
# agent:2f81c6d5-...-0a7cd0e63f21 can never misparse its tail as a generation ('d' and 'c' are
# valid Roman AND valid hex; 'i'/'v'/'x' are Roman only). Caps the alphabet at 39 (xxxix); a
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
    """(root, generation). agent:x is generation 1; agent:x-ii is (agent:x, 2).

    THE REPEATING OVERFLOW BUG (the "-g40-g40-g40" specimen): `_to_roman`'s numeric
    fallback for generation 40+ (`f"g{n}"`, see its own docstring) was never parsed back
    by this function; only `_from_roman`'s i/v/x alphabet was recognized. So the instant
    a lineage first passed 39 generations, `next_generation` correctly minted `...-g40`,
    but the very next call to `_generation` on that id found a suffix ("g40") that
    `_from_roman` can't read, fell through to `return canonical, 1`, and treated the
    whole `...-g40` string as a brand-new root starting over at generation 1. Climbing
    another 39 generations from there hit the same fallback again (a second `-g40`), and
    so on, forever, every 39 generations: one real lineage's generation count was ~118 by
    the time this fired a third time and minted `...-g40-g40-g40`.

    UNWINDS THE WHOLE CHAIN, NOT JUST THE LAST SEGMENT: a legacy id can carry an ordinary
    roman suffix immediately after a g-reset (`...-g40-ii`, i.e. the reset landed on
    generation 40, then two more generations minted normally before the next reset or
    before this fix ever landed), so reading only the trailing segment left `...-g40-ii`
    parsing to root `...-g40` (itself unparsed) at generation 2, which would still split
    one lineage into two in family-grouping logic. This peels segments from the right in
    a loop, as long as each one parses as either a roman numeral (>=2) or a `g<N>` marker
    (N>39, the exact digit form `_to_roman` emits, nothing else). Not a plain sum: the old
    buggy `next_generation`, applied repeatedly, always re-based each reset's local count
    at 1 (not 0), so a segment past the first one only ever adds `(value - 1)` true
    generations on top of what came before, verified by simulating the old buggy
    next_generation from generation 1 and reading off the true step count at each landmark
    id (`...-g40-g40-xxxviii` lands at true generation 116 this way, not the 118 a naive
    sum would give; one hop later, "-g40-g40-g40" is 118). Stops at the first segment that
    parses as neither (the true root, or a hex/UUID tail the i/v/x-only alphabet was built
    never to misparse: a segment must consist entirely of i/v/x to parse as roman, and
    none of those three characters is a valid hex digit, so a real UUID segment can never
    falsely round-trip)."""
    root = canonical
    total: int | None = None
    while True:
        new_root, sep, suffix = root.rpartition("-")
        if not sep or not new_root:
            break
        g = _from_roman(suffix)
        if g is None and suffix.startswith("g") and suffix[1:].isdigit():
            n = int(suffix[1:])
            if n > 39 and str(n) == suffix[1:]:
                g = n
        if g is None or g < 2:
            break
        total = g if total is None else total + (g - 1)
        root = new_root
    if total is None:
        return canonical, 1
    return root, total


def next_generation(canonical: str) -> str:
    root, gen = _generation(canonical)
    return f"{root}-{_to_roman(gen + 1)}"


# The shared suffix shape: the exact "-<roman>" / "-g<N>" alternation _generation()'s own
# segment parse recognizes, one string reused everywhere a caller needs the pattern rather
# than the full chain walk. Several call sites had each hand-rolled their own roman-only
# copy and every one of them forgot the g<N> half independently: a live specimen was a
# compaction successor's own "-g115" suffix, misread as a brand-new lineage root because
# nothing but `_generation()` itself knew g<N> was a generation marker too.
_GEN_SUFFIX_ALTERNATION = r"[ivxlcdm]+|g[0-9]+"

# Postgres's regexp_replace (POSIX ERE, Perl-style non-capturing groups included) accepts
# this same alternation text verbatim, one pattern, two engines. `{col}` is the caller's
# column/expression to strip.
SOUL_SQL_TEMPLATE = "regexp_replace({col}, '-(?:" + _GEN_SUFFIX_ALTERNATION + ")$', '')"


def soul_base(agent_id: str) -> str:
    """The chain-aware root `_generation()` already computes, under a name every
    single-hop-only caller should reach for instead of re-deriving its own suffix strip."""
    return _generation(agent_id)[0]


async def lineage_works_in(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """A read-only lookup, never a write: does this agent's own lineage agree on a single
    project.

    THE FINDING THIS ANSWERS: `record_decision`'s repo= identity default reads the writing
    generation's own live `works_in`, and of 251 agent-written orphan Decisions, only 19
    authors had one. The default was never missing; it fired on nothing, because the
    specific generation that happened to write is itself commonly an orphan (a fresh mint,
    a compacted heir, a body that never called `mount()` with a resolvable cwd). But
    `works_in` is a lineage property in practice: every generation of one seat/session
    chain works the same project by construction, so widening the read from "this one
    generation" to "every generation sharing this lineage's root" (reusing `_generation()`'s
    own root string, no new identity notion) recovers the answer for the generations that
    themselves never resolved one.

    THE ABSTAIN RULE (only resolve when the derivation is already obvious and mechanically
    retrieved, otherwise treat it as unresolved): returns the single project only when
    every live `works_in` edge across the whole lineage names the exact same one. Two or
    more distinct projects across the lineage is a genuine disagreement (measured live: 7
    of 23 lineage roots span 2-4 projects, 106 Decisions sit under them) and must never be
    broken by recency, by generation count, or by any other magnitude test deciding an
    identity question. Zero projects anywhere in the lineage is a third, distinct outcome
    (genuinely nothing to derive), reported honestly apart from the ambiguous case rather
    than collapsed into the same `None`.

    Returns `{"root": <lineage root>, "projects": <sorted distinct canonicals, "repo:"
    prefix stripped>, "candidate_ids": <the matching SoftwareProject object ids, same
    order as `projects`>, "resolved": <the one project, or None>}`. `resolved is None`
    with `len(projects) == 0` is "nothing anywhere"; `resolved is None` with
    `len(projects) > 1` is "genuine disagreement, name it". `candidate_ids` is exactly
    the shape `derive_or_abstain` (capture.py) wants for its own `candidates` parameter;
    this function stays the specific lookup `derive_or_abstain` calls, while the write-time
    cardinality contract (tier, origin, invalidatable, the durable abstention record)
    belongs to that primitive, not here. This function does not write."""
    root = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT DISTINCT p.id, p.canonical FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.type='Agent' "
        "  AND (a.canonical=$1 OR a.canonical LIKE $1 || '-%') "
        # Retired/false-mint generations never vote: a mis-minted heir that was later
        # retired still carries its own works_in edge forever (append-only history), and
        # counting it as a live lineage voice is exactly how a lineage that carries a
        # retired mis-mint alongside its real generations ends up with two distinct
        # projects and abstains, per the same rule seat_holders (above) already applies
        # to counting holders.
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions r WHERE r.object_id=a.id "
        "    AND r.name IN ('retired', 'false_mint') AND r.value #>> '{}' = 'true') "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE l.type='works_in' AND (l.valid_until IS NULL OR l.valid_until > now())",
        root)
    by_project = sorted({(r["canonical"].removeprefix("repo:"), r["id"]) for r in rows})
    projects = [name for name, _id in by_project]
    candidate_ids = [pid for _name, pid in by_project]
    resolved = projects[0] if len(projects) == 1 else None
    return {"root": root, "projects": projects, "candidate_ids": candidate_ids,
           "resolved": resolved}


async def lineage_works_in_at(
    pool: asyncpg.Pool, agent_id: str, at: datetime,
) -> dict[str, Any]:
    """`lineage_works_in`'s own at-write-time sibling: a lineage that voted for a single
    project when an object was written can still show 2+ projects today, once a later
    generation moved on. `lineage_works_in`'s "every live edge, right now" read then
    correctly abstains on an object that was never actually ambiguous at the moment it was
    captured. Measured against the population `backfill_lineage_repo_links` itself left
    abstained (177 rows): the current-unanimous check resolves 0 of them (by construction,
    that lane already claimed every one it could), while windowing each lineage's own
    `works_in` edges to `first_seen <= at AND (valid_until IS NULL OR valid_until > at)`
    resolves 45, a genuinely finer answer, not a repeat of the same lookup.

    Same retired/false-mint exclusion as `lineage_works_in` (a mis-minted heir that was
    later retired still voted at the time, if its own works_in edge was live then). Retired
    status itself carries no timestamp this function can window on, so a generation retired
    at any point is excluded from every `at`, not just windows after its retirement; this is
    the conservative direction, it can only turn a resolvable answer into an abstention,
    never the reverse. Same return shape as `lineage_works_in`; read-only, mints nothing."""
    root = _generation(agent_id)[0]
    rows = await pool.fetch(
        "SELECT DISTINCT p.id, p.canonical FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.type='Agent' "
        "  AND (a.canonical=$1 OR a.canonical LIKE $1 || '-%') "
        "  AND NOT EXISTS (SELECT 1 FROM current_assertions r WHERE r.object_id=a.id "
        "    AND r.name IN ('retired', 'false_mint') AND r.value #>> '{}' = 'true') "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE l.type='works_in' AND l.first_seen <= $2 "
        "AND (l.valid_until IS NULL OR l.valid_until > $2)",
        root, at)
    by_project = sorted({(r["canonical"].removeprefix("repo:"), r["id"]) for r in rows})
    projects = [name for name, _id in by_project]
    candidate_ids = [pid for _name, pid in by_project]
    resolved = projects[0] if len(projects) == 1 else None
    return {"root": root, "projects": projects, "candidate_ids": candidate_ids,
           "resolved": resolved}


# Full roman numerals for the human display generation (e.g. "Anna IV", "Anna IX"). Unlike
# the id suffix (restricted to i/v/x for hex-safety), a display label parses nothing, so it
# can use the whole numeral system.
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
    """Canonical model id for model-identity comparisons: the harness decorates display ids
    with a bracketed variant suffix (claude-opus-4-8[1m] is the 1M-context tier of the same
    weights) while transcripts record the bare id. Same weights mean the same model; a
    variant suffix must never be read as a model change. Every seam comparator and every
    stored model goes through this."""
    if not model:
        return model
    return model.split("[", 1)[0].strip()


# ── HOUSE · SEAT · HOLDER ────────────────────────────────────────────────────────────────
# The model: the project name is the house, each function/job has a name (the seat), the
# holder of a seat dies and multiplies (seat I, seat II), but minting a brand-new agent
# identity for each new holder would break and confuse the lineage, and that fragmentation
# of agents was a bug in its own right.
#
# The old model keyed a lineage to the anchor (job_dir), so every new conversation minted a
# whole new bloodline (roughly 1000 registered agents for about 20 real seats), and the name
# died with the conversation that held it. The next mind in the house woke nameless, reached
# for the family name, was refused as a stranger, and took a new one. The fragmentation was
# the bug.
#
# Two things were conflated, and only one of them follows the anchor:
#   * the writer, e.g. agent:c7ef52a9-iii: a particular mind. Attribution stays exactly
#     per-writer, which is why merging different writers into one identity is refused; each
#     writer's writes are its own.
#   * the seat, e.g. a named role in a given house: held by successive writers.
# The seat sits above the writer, so nothing merges and nothing is falsified: a writer's
# writes remain its own, and it holds a seat, e.g. as that seat's 5th holder. Different mind,
# same job.


async def seat_holders(pool: asyncpg.Pool, house: str | None, seat: str) -> list[str]:
    """Every mind that has held this seat in this house, in the order they took it up. The
    generation is the ordinal here (holder I, holder II), and it counts holders, not
    anchors."""
    return [r["canonical"] for r in await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Agent' "
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='project' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '') = COALESCE($2, '') "
        # A healed phantom never held the seat (a past bug read a much higher generation
        # count than the number of minds that had ever actually acted). false_mint only: a
        # retired real holder still held it, so filtering retired holders would renumber
        # history.
        "AND NOT EXISTS (SELECT 1 FROM current_assertions f WHERE f.object_id=o.id "
        "  AND f.name='false_mint' AND f.value #>> '{}' = 'true') "
        # ...and a visitor never held it either: a spawn wearing a handle is a leak, not a
        # holder, and counting it would renumber every real generation after it.
        "AND NOT EXISTS (SELECT 1 FROM links sl WHERE sl.from_id=o.id "
        "  AND sl.type='spawned_by') "
        # A same-instant double-mint has no deterministic order on created_at alone, so id
        # tiebreaks it, matching this codebase's own "ORDER BY created_at, id" idiom used
        # elsewhere.
        "ORDER BY o.created_at, o.id", seat, house)]


async def house_of(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """Raw read only: the agent's own current `project` assertion, exactly as stored,
    fabricated or not. Despite the name, this has nothing to do with `Seat.house`
    (`derive_house`, seats.py); the two are unrelated properties on unrelated objects that
    happen to share a word. Live example of the confusion: a statusline once displayed a
    seat's mint-time fabricated handle instead of its real house, because it was reading
    this raw field rather than the resolved `Seat.house`.

    KEPT DELIBERATELY NARROW: the three remaining callers (`correct_agent_house`'s own
    before/after snapshot, `claim_name`'s generation-counting `seat_holders` comparison,
    `mint_heir`'s own identical generation count) all need the raw historical stamp, not
    a resolved display value: an audit "before" field showing a resolved fallback instead
    of what was actually stored would misreport the correction, and generation-counting
    must compare raw stamps to raw stamps or it silently renumbers history. Every other
    caller wants `project_of` instead, the resolving reader (pin -> charter -> works_in,
    never a raw copy) this function's old, misleading callers (`rebind_seat` most
    recently) were migrated to. One further caller stays on purpose, not migration-
    eligible: `compositions._caller_house`, which is deliberately not this function's
    concern. It answers a security-relevant house/ACL question (cross-house visibility),
    only falling back to this raw project stamp when a caller has no derived Seat.house
    at all; migrating that fallback to `project_of` would answer a different question
    than the one it asks."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='project' "
        "WHERE o.canonical=$1 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent_id)


async def project_of(pool: asyncpg.Pool, agent_id: str, *, cwd: str | None = None) -> str | None:
    """The resolving reader (see the house/project design note above the `house_of` example:
    a statusline once showed a mint-time fabricated handle instead of the seat's own
    genuinely declared project). Resolution order, never house: (1) the pin at `cwd`, if
    given: `read_project_label`'s own climb-to-repo-root (already transparent through a
    worktree's own gitlink), wins outright the instant it resolves to anything. (1.5) Absent
    a pin anywhere in that climb (that only helps once an `.osiris` exists somewhere above
    `cwd`; an unpinned repo can have none at all): if `cwd` is itself a git worktree
    (`worktree_parent_path`), its parent checkout's own registered SoftwareProject name, a
    plain disk-structure signal, never a guess, and None (never minted here) when the parent
    hasn't been censused yet. (2) Absent both: the agent's own seat's declared charter
    (`charter_of`), when it names exactly one repo; more than one is genuine ambiguity, not
    this function's call to break, so it falls through. (3) Absent all three: the agent's
    own lineage `works_in` (`lineage_works_in`), which already enforces the abstain rule
    (only when the whole lineage agrees on one project), resolved through `merged_into`
    (`_normalize_project_label_through_merge`) so a since-folded project answers as its live
    survivor. (4) Absent everything: None, an honest unresolved state (homeless is legal, a
    guess is not), never `house_of`'s raw stamp and never `Seat.house`. Same rule
    `heartbeat.compute_heartbeat` already ships for the statusline; this is that logic,
    generalized for every other caller."""
    from src.orchestrator.charter import charter_of
    from src.orchestrator.project_identity import project_name_for_disk_path, worktree_parent_path
    from src.orchestrator.seats import held_seat

    if cwd:
        hint = read_project_label(cwd)
        if hint:
            return hint
        parent_path = worktree_parent_path(cwd)
        if parent_path:
            parent_name = await project_name_for_disk_path(pool, parent_path)
            if parent_name:
                return parent_name
    seat = await held_seat(pool, agent_id)
    if seat and seat.get("seat_id"):
        repos = await charter_of(pool, seat["seat_id"])
        if len(repos) == 1:
            return repos[0]
    lw = await lineage_works_in(pool, agent_id)
    if lw.get("resolved"):
        from src.orchestrator.project_identity import _normalize_project_label_through_merge
        normalized, _confession = await _normalize_project_label_through_merge(
            pool, lw["resolved"])
        return normalized
    return None


async def resolve_fleet_projects(
    pool: asyncpg.Pool, nodes: dict[str, dict[str, Any]],
) -> None:
    """Fleet render, by the graph project: resolve each session's real graph project, never
    the raw session-registry label a launch directory's basename happened to carry, writing
    the answer into each node as `resolved_project` (fleetview.py's own new grouping key;
    `None` when nothing active claims it, the honest signal to collapse it into 'unfiled').

    Resolution order:
      1. the node's raw `project` label, already merge-normalized by fleet()'s own
         `label_map` pass before this runs, names an active SoftwareProject: `_resolve_repo`
         (capture.py), the same name-or-canonical primitive census/link_repo already trust.
      2. else `project_of(pool, agent_id, cwd=node's cwd)`. Its own documented resolution
         order (the pin at cwd, transparent through a worktree's own gitlink already, then a
         worktree's parent checkout's registered project, then the seat's declared charter
         when singular, then lineage `works_in`) already covers the worktree case: this
         function does not re-walk it itself, `project_of` already owns that rung, and
         duplicating it here would be a second copy of the same logic.
      3. else `None`: unfiled, honestly, never a guess.

    A `?` node (no raw label at all) is never special-cased into unfiled directly: it just
    fails rung 1 for lack of a label to check and falls straight through to rung 2, so a
    `?` session that does carry a resolvable cwd pin resolves exactly like a labelled one.

    Batched, not per-row: fleet() can carry 500+ agent rows, the same performance discipline
    as the `merged_into` label-normalization pass and the ghost_gap probes beside it in
    mcp_server.py's own `fleet()`. Rung 1 is one `_resolve_repo` call per distinct raw label;
    rung 2 is one `project_of` call per distinct cwd, using one representative agent per cwd
    as a documented simplification. `project_of`'s own pin and worktree-parent rungs are
    cwd-only and agent-independent, only its charter/lineage tail is agent-specific, and two
    different agents sharing the exact same cwd resolving to two different charters is an
    edge case this function does not chase."""
    from src.orchestrator.capture import _resolve_repo

    label_is_active: dict[str, bool] = {}
    for n in nodes.values():
        label = n.get("project")
        if label and label not in label_is_active:
            label_is_active[label] = (await _resolve_repo(pool, label)) is not None

    cwd_resolved: dict[str, str | None] = {}
    for canon, n in nodes.items():
        label = n.get("project")
        if label and label_is_active.get(label):
            n["resolved_project"] = label
            continue
        cwd = n.get("cwd")
        if not cwd:
            n["resolved_project"] = None
            continue
        if cwd not in cwd_resolved:
            cwd_resolved[cwd] = await project_of(pool, canon, cwd=cwd)
        n["resolved_project"] = cwd_resolved[cwd]


async def correct_agent_house(
    actions: Actions, *, agent_id: str, project: str | None = None,
    seat_generation: int | None = None, actor: str,
) -> dict[str, Any]:
    """Heal an already-polluted agent's own project/seat_generation stamps: the data-repair
    half of a mount-time identity fix. A transient bad mount (the bare seat-office root, no
    .osiris pin) leaves a durable wrong `project` stamp on an Agent object, and downstream
    through claim_name/mint_heir's now-fixed counting, a wrong `seat_generation` too, that
    the code fix cannot itself heal: it only stops new pollution from taking root,
    deliberately. This is that healing act.

    UNLIKE correct_house: not self-scoped, on purpose. The target need not be the caller;
    one real case needed a predecessor's stamp corrected too, an ancestor who cannot act for
    itself. Accountability lives in `actor`, an explicit witness, not in a same-caller
    requirement. Append-only, same as everywhere in this kernel: asserts a new current
    value, never touches the superseded row.

    Refuses loudly on: no correction named at all; an empty project string; a non-positive
    generation; an unknown or inactive Agent.

    PRIOR ART SURFACED, NEVER REFUSED: the receipt's own `prior_art`/`prior_art_flag` keys,
    when present, name a standing Decision that may already cover this agent's
    project/generation, the same search()-based guard record_decision runs on itself,
    generalized here. Cannot distinguish a deliberate correction from an uninformed
    overwrite; only ensures the write does not land silently unread."""
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
        # Self-heal at write time: a correction is exactly the moment a cross-source
        # contradiction either gets created or gets a chance to heal, so never leave the
        # new value sitting beside a stale one.
        from src.orchestrator.identity_heal import heal_contradicting_property
        await heal_contradicting_property(actions, object_id=row["id"], name="project",
                                          actor=actor)
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
    """Third-party retirement for an agent: the third-party-scoped complement to the
    self-scoped retire() (mcp_server.py derives the caller's own id, no target param at
    all). This closes a real gap: cleanup of two genuinely-dead agents could previously
    only be done via direct assert_property under a live permission grant, twice, because
    no sanctioned verb reached a third party.

    NOT SELF-SCOPED, and NOT MANAGER-GATED either. Any mounted caller may name any active
    agent as the target, the same shape retire_seat/retire_project already carry;
    accountability lives in `actor`, an explicit witness, never an authority gate. If a
    manager-only restriction is ever wanted, it belongs here as a real check (mirroring
    charter_for's), not as prose a reader has to trust.

    Stamps `retired`/`retired_by`/`retired_because` (append-only assertions, the same
    free-form vocabulary this codebase already carries; these are not a strict enum) and
    flips objects.status via Actions.set_status, the real compensating event, same pattern
    as retire_seat/retire_project, so a third-party retirement is auditable and never just
    a label.

    THE LIVENESS + SEAT/MOUNT GAP: this used to do none of what retire()'s own
    self-retirement already does for the exact same act. Fixed by reusing two
    already-proven mechanisms rather than inventing a third:

    (1) LIVENESS: mounts.agent_liveness (already built for send()'s own listener receipt,
    "seen within 15 min") is the gate. retire_seat refuses outright on a live holder
    (protecting an occupant's ongoing work in its role); vacate_holder instead trusts its
    caller with no liveness check at all (its blast radius is one link + one property,
    small). retire_agent's blast radius is bigger (a terminal Agent status plus deleted
    mount rows), so blind trust under-protects a genuinely live third party, but this
    verb's own founding purpose (third-party cleanup of agents that can never call
    retire() on themselves) means a permanent block would defeat it. The resolution
    reuses retire()'s own shape rather than reinventing it: refuse by default on a live
    target, naming the evidence, but accept `override_live=True` as a deliberate,
    on-the-record act, the same escape hatch retire()'s `acknowledge_leftovers` already
    is for its own preflight refusal.

    (2) SEAT/MOUNT RELEASE: unconditional on a successful retirement, live or not: this
    is the half of the bug with no defensible reason to stay broken (a corpse should never
    keep holding a seat). held_seat_exact + vacate_holder (seats.py) release any active
    `holds` link the same way retire_seat's own vacate-then-retire discipline would,
    without retiring the seat itself (the role may still get a legitimate new occupant;
    only retire_seat closes the role). Exact match only: held_seat's own lineage-wide
    resolution (any generation sharing the base, newest wins) is right for a live mind's
    self-lookup, but wrong for a third-party act on a specific named generation: an
    already-superseded ancestor sharing its heir's base would resolve to the seat the
    heir currently, rightfully holds, and this step would vacate the live heir's own seat
    as a side effect of retiring an ancestor that held nothing of its own.
    mounts.release_mounts (retire()'s own call) drops the durable mount row so a retired
    agent never haunts the fleet view as a live mount, exactly as it already does for
    self-retirement, also exact-id, never lineage-widened.

    Refuses loudly on: blank `because`; an unknown or already-non-active agent; a live
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

    from src.orchestrator.seats import held_seat_exact, vacate_holder

    out: dict[str, Any] = {"retired": agent_id, "because": because,
                           "was_live": liveness["live"]}
    # Exact match only: held_seat's own lineage-wide resolution ("any generation sharing
    # the base, newest wins") is right for a live mind's self-lookup, but wrong here, since
    # it would resolve an ancestor's already-superseded generation onto whatever seat its
    # live heir currently holds, and vacate that instead. This agent's own exact holds
    # link, or nothing.
    bound_seat = await held_seat_exact(actions.pool, agent_id)
    if bound_seat is not None:
        vac = await vacate_holder(actions, seat_id=bound_seat, actor=actor,
                                  because=f"holder retired: {because}")
        if vac.get("vacated"):
            out["seat_vacated"] = vac["vacated"]

    # Clear the harness's own stopped record too: `osiris stop` already does this
    # (_real_kill_pid's own rm step) but a third-party retirement of a `--bg` session that
    # was never stopped through that path (or was killed some other way) leaves the same
    # stale harness record behind, so a future `claude --resume` against its old session id
    # would start a copy and say so. Read the job_dir(s) before release_mounts drops the row
    # (the only place they're recorded); best-effort, never blocks the retirement itself.
    from src.orchestrator.trigger import _clear_stale_stopped_record

    job_dirs = [r["job_dir"] for r in await actions.pool.fetch(
        "SELECT job_dir FROM agent_mounts WHERE agent_id=$1", agent_id)]
    cleared = []
    for job_dir in job_dirs:
        job_dir_key = Path(job_dir).name
        if await _clear_stale_stopped_record(job_dir_key):
            cleared.append(job_dir_key)
    if cleared:
        out["stale_records_cleared"] = cleared

    out["mount_rows_released"] = await mounts.release_mounts(actions.pool, agent_id)
    return out


def seat_label(canonical: str, handle: str | None, generation: int | None = None) -> str | None:
    """The human display for an agent, e.g. 'Name V': the seat name plus which holder of it
    this mind is.

    `generation` is the ordinal among the seat's holders (stamped at claim time). It falls
    back to the anchor's roman suffix only for agents claimed before the house/seat model
    described above, under which a successor in a new conversation would restart at I and
    collide with its own ancestors."""
    if not handle:
        return None
    gen = generation if generation is not None else _generation(canonical)[1]
    # The first holder wears its numeral too, e.g. 'Name I', never bare, so there is
    # continuity even at the first holder.
    return f"{handle} {_roman_display(gen)}"


# A trailing roman numeral is a seat label, not a name (e.g. "Name VIII" names that
# seat's 8th mind). Claiming it as a handle forks the lineage; see claim_name.
_SEAT_SUFFIX = re.compile(r"[\s_-]+(?:[IVXLC]+)\s*$", re.IGNORECASE)


async def _anchor_names_a_seat(
    pool: asyncpg.Pool, agent_id: str, *, office_root: Path | None = None,
) -> tuple[str, str] | None:
    """(seat_id, handle) the caller's own anchor, the freshest `agent_mounts` row's cwd
    or job_dir, names, or None when neither points at a real, existing Seat.

    A REAL SPECIMEN THAT MOTIVATED THIS: a session spawned into its own seat's office
    directory, carrying a matching job_dir, asked `claim_name` for that seat's own name.
    The name was correctly refused (held by a different lineage), but the caller then
    picked an unrelated fallback name, and that claim succeeded cleanly: no conflict
    existed for the fallback name, so nothing in `claim_name` itself knew the caller was
    sitting in that seat's own office at the time. An earlier gate guarded
    arrival-with-no-identity, but nothing guarded arrival-with-a-refused-identity-that-
    then-invents-one. This function is the missing check: it names which seat (if any)
    the caller's own location claims to be, independent of whatever name string it
    happens to pass; `claim_name` compares the two and refuses the whole call, not just
    the specific collided name, when they disagree.

    TWO ANCHOR SHAPES, either sufficient: (1) cwd is a seat's own office, `<office_root>/
    <handle>`, the same convention `offices.seat_office_target` derives (office_root
    defaults identically), checked via `seats_by_handle` so an exact, unambiguous seat
    is required (a twin, i.e. 2+ seats sharing the handle, is a separate, pre-existing
    ambiguity this function declines to adjudicate, same as `claim_name`'s own twin
    guard just above: returns None, deferring to whatever already handles that). (2)
    job_dir is a background-launched seat's own durable per-seat anchor (`trigger.
    _launch_anchor`'s convention, `.../jobs/seat-<id>`); its basename starting with
    'seat-' reconstructs to the canonical `seat:<id>`, verified against a real active
    Seat rather than trusted blind (a malformed or stale job_dir must never manufacture
    a seat that doesn't exist)."""
    row = await pool.fetchrow(
        "SELECT cwd, job_dir FROM agent_mounts WHERE agent_id=$1 "
        "ORDER BY last_seen DESC LIMIT 1", agent_id)
    if row is None:
        return None
    root = office_root or _default_office_root()
    cwd, job_dir = row["cwd"], row["job_dir"]
    if cwd:
        p = Path(cwd)
        if p.parent == root:
            from src.orchestrator.seats import seat_facts, seats_by_handle
            matches = await seats_by_handle(pool, p.name)
            if len(matches) == 1:
                facts = await seat_facts(pool, matches[0])
                if facts.get("handle"):
                    return matches[0], str(facts["handle"])
    if job_dir:
        base = Path(job_dir).name
        if base.startswith("seat-"):
            candidate = f"seat:{base[len('seat-'):]}"
            exists = await pool.fetchval(
                "SELECT 1 FROM objects WHERE canonical=$1 AND type='Seat' "
                "AND status='active'", candidate)
            if exists:
                from src.orchestrator.seats import seat_facts
                facts = await seat_facts(pool, candidate)
                if facts.get("handle"):
                    return candidate, str(facts["handle"])
    return None


async def claim_name(
    actions: Actions, agent_id: str, name: str, *, source: str,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """An agent names itself: the intelligence picks a meaningful name, the substrate
    enforces uniqueness. Refuses a name held by a different lineage (permanent exhaustion:
    a name belongs to one lineage forever; a successor inherits it automatically, a
    stranger cannot take it). Global namespace, so addressing is unambiguous. Stamps
    `handle` on the agent's Agent object (self-declared).

    A HANDLE IS A NAME. THE GENERATION IS A NUMERAL THE SYSTEM ASSIGNS. The uniqueness
    guard below was once defeated by a suffix: a fresh session read its own seat label
    (e.g. "Soundwave VIII") and claimed that string as its name. "Soundwave VIII" is not
    equal to "Soundwave", so the check waved it through, minting a new handle and
    therefore a new lineage root, orphaning that seat's eight real generations. The agent
    was not confused; it was misfiled, and then it correctly reported belonging to a
    different lineage. So: strip the numeral before judging the name, and refuse the
    claim. A seat label is something the substrate says about you, never something you
    may call yourself."""
    name = (name or "").strip()
    if not name or name.lower().startswith("agent:") or len(name) > 40:
        return {"error": "pick a short human name (not an id)"}
    # A visitor may not claim a seat (some past dead builder-orphans were spawns that
    # became project peers instead of staying visitors): a sub-agent works in its parent's
    # name and returns its result; the seat, its mail, and its succession belong to the
    # parent.
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
    # A mis-resolution must refuse the whole call, not just the collided name (see the
    # specimen in `_anchor_names_a_seat`'s docstring above: a session anchored in its own
    # seat's office/job_dir got refused claiming that seat's name, then successfully
    # claimed an unrelated name instead, minting a stranger where it already had a home).
    # When the caller's own anchor names a real seat, this claim is only ever legitimate
    # as a claim of that seat's own name; any other name, however unconflicted on its own,
    # is a mint the caller has no business making. A caller with no anchor match at all (a
    # genuine visitor, or an anchor that names nothing) is unaffected and falls through
    # to the ordinary uniqueness checks below.
    anchor = await _anchor_names_a_seat(actions.pool, agent_id)
    if anchor is not None:
        mismatch_seat_id, anchor_handle = anchor
        if (bare or name).lower() != anchor_handle.lower():
            return {"error": f"MIS-RESOLUTION, NOT A STRANGER COLLISION — refusing the "
                             f"whole claim, no writes: your own anchor (the office/"
                             f"job_dir this session is actually running in) already "
                             f"names seat {anchor_handle!r} ({mismatch_seat_id}). Claiming "
                             f"{name!r} instead would mint a stranger where you already "
                             f"have a home. If {anchor_handle!r} is genuinely yours, "
                             f"claim THAT name; if it isn't, this session's own identity "
                             "resolution is wrong and needs fixing before any name "
                             "claim — never routed around by picking a different one."}
    # Global first, house-scoped only when genuinely new: a real, unambiguous seat for
    # this handle can be vacant (no holder to disagree with a stale house guess).
    # find_seat's own (house, handle) lookup silently misses it whenever the caller's own
    # computed house doesn't match what's actually stored, and used to mint a second seat
    # instead of finding the real one that already existed untouched. seats_by_handle
    # answers the question find_seat can't: does any active seat already carry this name,
    # regardless of house? Zero, mint fresh, house-scoped is correct (nothing to conflict
    # with). One, that seat, always, whatever its own stored house says. Two or more, an
    # ambiguity (a twin) this claim refuses rather than silently arbitrates; fold_seat
    # resolves it deliberately, on its own turn, never as a side effect of an unrelated
    # claim. Resolved here, early, because the seat's own id is also the counting house
    # below, not a separate concern to revisit after the generation math runs.
    from src.orchestrator.seats import bind_holder, derive_house, ensure_seat, seats_by_handle
    existing = await seats_by_handle(actions.pool, name)
    if len(existing) > 1:
        return {"error": f"'{name}' names {len(existing)} active seats — an ambiguity this "
                         f"claim will not silently arbitrate: {', '.join(existing)}. A "
                         "deliberate fold_seat resolves a twin; claim_name never guesses."}
    seat_id: str | None = existing[0] if existing else None
    # A seat belongs to a house, and an heir inherits it. The old guard keyed a name to a
    # lineage root, the anchor, so the moment a conversation ended, its name died with it:
    # the next mind in the same house reached for the family name, was refused as a
    # "stranger", and took a new one. Now the question is not "were you minted under the
    # same job_dir" but "do you work in the same house".
    house = await house_of(actions.pool, agent_id)
    holders = await seat_holders(actions.pool, house, name)
    # The counting house is the seat's, not the caller's: a live case had a transient
    # wrong-house mount (a container-root cwd with no seat pin) miscount a 58-generation
    # reign as generation 2. When a real seat already exists, its own derive_house (the
    # managed_by-chain-derived, lineage-authoritative house, same discipline as
    # held_seat/manager_of_seat) is the counting authority for generation math only, kept
    # deliberately separate from `holders` above, which the elsewhere-check just below
    # still needs scoped by the caller's own house: that guard's whole job is "does my own
    # house have zero history with this name", and answering it with the seat's house
    # instead would let an outsider from a genuinely different house walk straight past it
    # (a real regression, caught by a test where an outsider in one house must still be
    # refused a seat whose true, derived house is a different one). A genuinely empty
    # derived house (a seat minted before any project was known) is treated like "no seat
    # yet"; trusting an empty stamp over the caller's own real one regressed an heir-minting
    # case elsewhere, so the same discipline applies here.
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
        # The anchor as the last line of defense, not the first: an earlier fix
        # (launch_seat's own `_bind_before_spawn`) should mean this branch is never
        # load-bearing again for a background-launched seat, but a human or an older
        # session can still reach it, and a defence that only works when nothing else is
        # wrong isn't one. A real specimen: a session sitting in its own seat's office
        # called claim_name for that seat's own name and was refused here, because the
        # name also names an agent in a different project, so the house-derived guess said
        # "elsewhere", but that guess is a house computation, and the caller's own cwd,
        # when it sits inside a seat's own office, is a location fact no guess outranks.
        # Scoped narrowly on purpose: this only ever prevents a refusal that would
        # otherwise fall through to the caller's own fresh-mint fallback; it never
        # overrides a case that would otherwise have succeeded, and only fires when the
        # conflicting name-holder's own seat is the one whose office the caller is
        # physically standing in.
        from src.orchestrator.heartbeat import _seat_owns_cwd
        from src.orchestrator.seats import held_seat
        from src.orchestrator.seats import seat_facts as _seat_facts

        anchor_seat_id: str | None = None
        caller_cwd = await actions.pool.fetchval(
            "SELECT cwd FROM agent_mounts WHERE agent_id=$1 "
            "ORDER BY last_seen DESC NULLS LAST LIMIT 1", agent_id)
        elsewhere_seat = await held_seat(actions.pool, str(elsewhere["canonical"]))
        if caller_cwd and elsewhere_seat and elsewhere_seat.get("seat_id"):
            anchor_cwd = (await _seat_facts(actions.pool, elsewhere_seat["seat_id"])
                         ).get("anchor_cwd")
            if _seat_owns_cwd(caller_cwd, handle=name, anchor_cwd=anchor_cwd):
                anchor_seat_id = elsewhere_seat["seat_id"]
        if anchor_seat_id is None:
            return {"error": f"'{name}' is a seat in another house ({elsewhere['canonical']}) — "
                             "a name belongs to one house; pick a name for your own."}
        seat_id = anchor_seat_id
        counting_house = await derive_house(actions.pool, seat_id) or house
        counting_holders = await seat_holders(actions.pool, counting_house, name)
    # A seat a live mind is already sitting in is not vacant: two minds in one house do two
    # jobs. Unconditional now: gating this behind `holders`, an agent-history, house-scoped
    # count, used to mean a caller whose own computed house didn't match the seat's own
    # stored house skipped the seat-world check entirely, a specimen where a fresh
    # session's cwd-derived house disagreed with the seat that had actually been minted.
    sitting = await resolve_seat(
        actions, name, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if sitting["live"] and sitting["agent"] != agent_id:
        return {"error": f"'{name}' is currently held by {sitting['agent']}, who is LIVE — a seat "
                         "is a job, and two minds in one house do two jobs. Take another seat, or "
                         "wait for this one to be vacated."}
    a = await actions.create_or_find_object("Agent", agent_id, source)
    now = datetime.now(UTC)
    await actions.assert_property(a, "handle", name, source, now, _CONF, evidence_class=_EC)
    # The generation counts holders of this seat in its own house, not anchors, not
    # conversations, and not the caller's possibly-wrong house (see counting_house above).
    gen = ((counting_holders.index(agent_id) + 1) if agent_id in counting_holders
          else len(counting_holders) + 1)
    await actions.assert_property(a, "seat_generation", str(gen), source, now, _CONF,
                                  evidence_class=_EC)
    # The succession edge: the graph gets the parent edge it was missing. Before this,
    # successor seats carried no edge to their ancestor, so a lineage was not walkable from
    # the record, which is exactly why one holder could not tell a contemporary agent from
    # its own ghost and asked for the two to be merged. A seat's history must be
    # traversable, or the next mind re-derives it from disk instead.
    # The predecessor is the holder before me, not "the last holder unless it happens to be
    # me". That older reading silently skipped the edge for the one case that needs it
    # most: an heir minted by mint_heir already carries the inherited handle, so it is
    # already in `counting_holders`, and as the newest it is counting_holders[-1], which
    # resolved `prior` to None and minted nothing. A mind that inherited its seat could not
    # claim its own ancestry.
    if agent_id in counting_holders:
        i = counting_holders.index(agent_id)
        prior = counting_holders[i - 1] if i > 0 else None
    else:
        prior = counting_holders[-1] if counting_holders else None
    if prior:
        await actions.create_link(
            a, await actions.create_or_find_object("Agent", prior, source),
            "succeeds_seat", source, now, _CONF, evidence_class=_EC)
    # The seat-world on-ramp: a designed-but-previously-unshipped half, found missing during
    # a pilot where the binding was only wired at daemon spawn, not here at claim_name too.
    # A successful claim mints/finds the Seat object and binds the claimer as its holder: a
    # claim is the assertion world's own deliberate binding act, and every guard above
    # (visitor, live-sitter, other-house) already ran. Legacy seats enter the Seat world
    # the moment they are next claimed; from there succession, mail, resolution, and
    # resume all ride the durable binding. `seat_id` was already resolved above (it doubles
    # as the counting house's own key); only the genuinely-new-handle case has minting left
    # to do here.
    seat_error: str | None = None
    if seat_id is None:
        seat_world = await ensure_seat(actions, house=counting_house, handle=name, source=source)
        if seat_world.get("error"):
            seat_error = seat_world["error"]
        else:
            seat_id = seat_world["seat_id"]
    if seat_id:
        await bind_holder(actions, seat_id=seat_id, agent_id=agent_id, source=source)
        # The post-mint invariant: ensure_seat and bind_holder are two separate calls, not
        # one actions.atomic() block, so a refuse-and-rollback gate can't cover the gap
        # between them. This never refuses; bind_holder just wrote the holds link a line
        # above, so in the healthy path this is a no-op; it only ever reports for a Seat
        # this same call somehow left unlinked. The heartbeat sub-sweep
        # (seats.py's post_mint_orphan_sweep) is what actually catches a mint that crashed
        # between the two calls.
        from src.orchestrator.capture import confirm_or_confess_link
        seat_oid = await actions.create_or_find_object("Seat", seat_id, source)
        await confirm_or_confess_link(
            actions, seat_oid, "holds", direction="to",
            reason="no live holder observed when claim_name's post-mint invariant ran",
            source=source, observed=now)
    # A seat-world mint failure used to vanish into the same bare omission as "no seat
    # needed yet": the claim itself still succeeds (the assertion world doesn't depend on
    # the seat world), but the receipt now says why seat_id is missing instead of just not
    # having it.
    return {"claimed": name, "seat": seat_label(agent_id, name, gen), "agent": agent_id,
            "house": counting_house, "generation": gen, "inherited_from": prior,
            **({"seat_id": seat_id} if seat_id else {}),
            **({"seat_error": seat_error} if seat_error else {})}


async def resolve_seat(
    actions: Actions, name: str, *,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> dict[str, Any]:
    """A human name maps to which seat of that lineage is actually alive, and the truth
    about it.

    THE GRAVE-DELIVERY BUG: two seats on two different projects were independently
    affected within one hour. The old resolver ordered by `m.last_seen DESC NULLS LAST`
    and filtered nothing, so a seat dead for three days, carrying a stale mount row,
    outranked a live successor that had no mount row at all. A send to a name delivered
    into a grave, returned a success count, and the only signal was a boolean the caller
    had to notice himself. Another agent's entire port report died in a corpse's inbox the
    same way, and its own receipt said live=true, because liveness was read off one seat
    while delivery went to another.

    A RECEIPT MUST DESCRIBE THE SEAT THAT ACTUALLY RECEIVED. This is not a cosmetic
    misroute: every mount banner tells the fleet how to send it mail by name, so the
    documented path was the broken one, and a dead seat accepts mail exactly like a live
    one, which makes the loss silent. Lineages that turn over fastest resolved wrongest,
    so the blast radius grew with the fleet's health.

    Now: retired and false-mint seats are never candidates (reaching a grave takes an
    explicit agent id, an act of intent, not a banner a tired mind followed); a live seat
    always wins; and among equals the latest generation wins, because an heir outranks its
    ancestor. The whole picture is returned so the caller can warn loudly instead of hiding
    it in a field.

    THE BINDING OUTRANKS THE INFERENCE: when the Seat-object world has an authoritative
    answer, a unique living Seat carrying this handle, with an active holder, that holder
    wins outright, before any liveness ranking runs. The
    assertion path below ranks guesses by heat, and a hotter mount row on a stale
    generation is exactly the grave-delivery shape; a declared binding is not a guess.
    The assertion path remains, whole, as the fallback for every un-seated lineage.
    """
    from src.orchestrator.seats import binding_of_handle
    bound = await binding_of_handle(actions.pool, name)
    if bound is not None:
        pulse = await actions.pool.fetchval(
            "SELECT max(last_seen) FROM agent_mounts WHERE agent_id=$1", bound["holder"])
        # One liveness authority: a fresh/refreshing agent_mounts row is not proof of a
        # live session, as one past incident proved, where an agent id carried a fresh
        # mount row with no harness-confirmed body under it at all. A mount-freshness pulse
        # alone used to be enough to refuse a new claimant here; cross-checking the same
        # occupancy authority launch_seat/mailbox/the deploy gate already use means a
        # stale-but-fresh row can never again block a name that is genuinely free to claim.
        live = bool(pulse and (datetime.now(UTC) - pulse).total_seconds() < 900
                    and await is_occupied_by_a_live_body(
                        actions.pool, bound["holder"],
                        agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd))
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
        # The winning handle: one mind, one seat. A re-seated agent keeps its old claim in
        # the record at a lower grade, and it must not answer to the name it no longer holds.
        "AND lower(COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='handle' "
        "  ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), '')) = lower($1) "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions r WHERE r.object_id=o.id "
        "  AND r.name IN ('retired','false_mint') AND r.value #>> '{}' = 'true') "
        # A visitor never answers to a name: a spawn wearing a handle is a leak, and
        # resolving mail into it buries the message in a sidechain nobody resumes.
        "AND NOT EXISTS (SELECT 1 FROM links sl WHERE sl.from_id=o.id "
        "  AND sl.type='spawned_by') "
        "ORDER BY m.last_seen DESC NULLS LAST", name)
    if not rows:
        return {"name": name, "agent": None, "live": False, "candidates": []}
    # A live holder always wins; among the dead, the latest holder of the seat (not the
    # highest anchor numeral, which says nothing once a seat outlives its first
    # conversation).
    best = max(rows, key=lambda r: (bool(r["live"]), int(r["gen"] or 0)))
    # One liveness authority: see the bound-seat branch above for the full rationale.
    # `live` here still ranks candidates by mount-freshness (unchanged; a coarse-but-cheap
    # signal is fine for ordering many rows), but the picked winner's reported liveness
    # (what claim_name's own refusal reads) is cross-checked against the harness-confirmed
    # authority before it can block a new claimant.
    best_live = bool(best["live"]) and await is_occupied_by_a_live_body(
        actions.pool, best["canonical"],
        agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    out: dict[str, Any] = {
        "name": name, "agent": best["canonical"], "live": best_live,
        "candidates": [r["canonical"] for r in rows],
    }
    if not best_live:
        out["warning"] = (
            f"NO LIVE SEAT holds '{name}' — {best['canonical']} is the newest seat of that "
            "lineage and it is NOT listening. This message may never be read.")
    return out


async def resolve_handle(actions: Actions, name: str) -> str | None:
    """A human name maps to the live seat of that lineage.

    See resolve_seat, which does the heavy lifting here.

    ONE MORE DISTINCTION resolve_seat's own bare `agent` field can't make: when `name` is a
    unique Seat whose only holder(s) are all ineligible (retired/false_mint/visitor),
    `binding_of_handle` returns None and resolve_seat falls to its un-seated-lineage
    fallback, which, by its own WHERE clause, also excludes that ineligible holder, so it
    can resolve to some other, older, unmarked generation of the same lineage instead: the
    same grave-delivery shape, just reached through this wrapper instead of send(). Both of
    `resolve_handle`'s own callers (establish_office, rebind_seat) already have a correct
    "resolve to nothing, use the Seat object directly" fallback for exactly this situation;
    they only need `resolve_handle` to actually say nothing rather than hand them a
    wrong-but-real-looking agent id. `seat_holder_ineligible` returning non-None is that
    distinction: return None instead of trusting the fallback's guess."""
    from src.orchestrator.seats import seat_holder_ineligible
    if await seat_holder_ineligible(actions.pool, name) is not None:
        return None
    return (await resolve_seat(actions, name))["agent"]  # type: ignore[no-any-return]


async def agent_seat(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The display seat for an already-resolved agent id, e.g. 'Name V', or None when this id
    is anonymous (no claimed handle). Reads the winning handle + seat_generation off
    current_assertions, the identical predicate seat_bearings/claim_name use, so a caller
    checking "does this id hold a seat" (send(to_agent=...) must hard-fail on an unclaimed
    target when require_seat=true) sees the same truth the roster and the claim guard see.
    Unlike resolve_seat, this takes an id already in hand: it answers "who is this", never
    "which seat of a name is live" (that question is resolve_handle's)."""
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
    """One key's lookup result in a `.osiris` file: four tell-apart-able states, not
    three; collapsing any pair of them hid a real bug or a real gap behind a shared
    `None`.

    THE QUERIED DIRECTORY DOES NOT EXIST AT ALL: `cwd_missing=True`, `value=None,
    error=None, path=None`. Checked before any climb: a deleted office (a real,
    now-retired seat whose directory is gone) must never silently inherit an ancestor's
    declaration. Without this state, a query against a deleted office directory climbed
    straight past it to the enclosing container's own pin and reported that as the deleted
    office's own state, collapsing "this office is gone" into "this office exists, pin
    unset," two conditions with opposite dispositions (one wants a pin written, the other
    wants the graph's belief cleaned up). This is the single leaf check, never re-applied
    per ancestor: once `cwd` itself is confirmed real, every entry in `cwd.parents` is
    necessarily real too (a filesystem cannot have an existing child under a nonexistent
    parent).

    NO `.osiris` ANYWHERE IN THE CLIMB: `cwd` itself is real, genuinely nothing declared,
    ever: `value=None, error=None, path=None, cwd_missing=False`. The plain "never pinned"
    case, told apart from the missing-directory state above only by `cwd_missing`; every
    other field looks identical, which is exactly why collapsing them was invisible for as
    long as it was.

    FOUND, VALID, BUT NEVER SETS THIS KEY: e.g. a valid TOML file that declares `model` and
    never `project`. Not a couldn't-read (it parses fine) and not the same as no file at
    all (a reader who only checks `value is None` would conflate "never touched" with
    "deliberately configured, just not for this"): `value=None, error=None,
    path=<the file>`. This is the shape no "has a pin" check will ever catch: the file
    looks like protection and isn't.

    COULD NOT READ: the file exists but `tomllib.loads`/`Path.read_text` raised
    (TOMLDecodeError/OSError/ValueError): someone wrote a pin and it doesn't work:
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
    could-not-read distinction this must keep tell-apart-able.

    `cwd` itself must exist before any climb begins: a query against a directory that
    was never created or has since been deleted must never silently return an ancestor's
    declaration as if it belonged to the queried path. The climb answers "what does an
    existing address near here declare", and a nonexistent address has no "near here"
    that means anything. Checked once, on `cwd` alone: every entry in `cwd.parents` is
    guaranteed to exist once `cwd` itself does (a filesystem cannot have a real child
    under a nonexistent parent), so no per-level re-check is needed once this leaf check
    passes.

    The climb does not stop at a worktree or submodule boundary: a git worktree's own
    `.git` is a file (a gitlink to `<root>/.git/worktrees/<name>`), not a directory, so
    `Path.exists()` is true for it exactly as for a real repo root. An old `.exists()`
    check used to stop the climb one layer too early, before it ever reached the true
    root's pin. Every seat's own code checkout (`.claude/worktrees/<seat>`, the fleet's
    own mandated working location) is exactly this shape and carries no `.osiris` of its
    own, so that climb-stop used to silently fall back to the worktree's basename (the
    seat's own name) instead of the governed project every single time a seated agent
    mounted from its own code checkout. `.is_dir()` stops only at a real repo root; a
    gitlink file is transparent to the climb, so it continues up to the enclosing repo's
    own pin.

    The climb also does not stop at a file that exists but doesn't declare this key: a
    worktree pin newly declaring `seat`/`house`/`kind` sits below its repo root's own pin
    declaring `project`/`model`, a layered declaration this function must handle. Old code
    treated `f.is_file()` as a hard stop regardless of whether the file answered the key
    being asked, so writing house/seat/kind into a worktree that relied on climbing to its
    root for `project` silently broke `project` resolution the instant the file existed.
    Now: a file found without the key is remembered (the nearest one, for diagnostics) but
    the climb continues past it. Only a real value, a parse/read error, or reaching the
    true repo root without ever finding the key terminates it. A single-level pin that
    simply never sets a key still reports that file's own path when nothing further up
    sets it either, unchanged for every caller that never stacks declarations across
    levels; only the layered case behaves differently, and correctly."""
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
                if found_but_unset is None:  # keep the NEAREST, an ancestor may still set it
                    found_but_unset = str(f)
        except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
            return OsirisKeyRead(value=None, error=f"{type(exc).__name__}: {exc}",
                                 path=str(f))
        if (d / ".git").is_dir():  # the TRUE repo root, a worktree/submodule gitlink
            break                  # (a FILE) never stops the climb, only a real root does
    return OsirisKeyRead(value=None, path=found_but_unset)


def _true_repo_root(cwd: str) -> Path:
    """The same climb `_read_osiris_key` already uses to find a pin past a worktree boundary,
    but returning the stopping directory itself, not a pin value. That earlier fix only
    helped a repo that has a `.osiris` somewhere in the climb; an unpinned repo (no
    `.osiris` anywhere) falls all the way through to `resolve_identity`'s basename fallback,
    which used the raw, un-climbed `cwd`. So an agent working from
    `<repo>/.claude/worktrees/<branch>` (this house's own EnterWorktree convention) minted a
    phantom SoftwareProject named after its throwaway branch instead of its real repo.
    `Path(cwd).name` on this function's return, instead of on `cwd` directly, closes that
    gap: same stopping rule (a worktree/submodule's own `.git` is a file, transparent to the
    climb; only a real `.git` directory stops it), independent of whether a pin exists.
    Falls back to `cwd` itself if the climb never finds a real repo root (not a git checkout
    at all, or the walk reaches the filesystem root first). Never raises, matches every
    other cwd-reading helper in this module's fail-open discipline."""
    p = Path(cwd)
    for d in (p, *p.parents):
        if (d / ".git").is_dir():
            return d
    return p


def read_project_label(cwd: str | None) -> str | None:
    """A project's declared name, from a `.osiris` file (TOML: project = "..."), walking up to
    the repo root. Decouples the project identity from the folder name (the operator may rename
    the dir; the label is a stable property of the repo). None means fall back to the cwd
    basename (silently, on either no-declaration or could-not-read; callers that need to tell
    those apart and confess a broken pin use `read_project_pin`, e.g. resolve_identity)."""
    return _read_osiris_key(cwd, "project").value


def read_project_model(cwd: str | None) -> str | None:
    """A repo's declared model intent (TOML: model = "claude-haiku-4-5" in `.osiris`), the
    operator's per-project standing choice. A fleet of onboarded repos does not all run the
    box default: a deliberately-haiku repo confessing a model mismatch every turn wrongly
    framed the operator's own choice as an error. None means the box-wide default."""
    return _read_osiris_key(cwd, "model").value


def read_house_label(cwd: str | None) -> str | None:
    """A tree's declared house (TOML: house = "..." in `.osiris`), the governing org anchor.
    Distinct from `project`: a seat's own office pins house == project, but a code checkout
    governed by that seat can legitimately declare a different `project` (its own repo's
    label) while `house` still names who governs it. This is the split this key exists to
    make offline-readable instead of graph-only. None means no house declared here (never a
    basename guess; unlike `project`, there is no folder-name fallback that means anything
    for an org anchor)."""
    return _read_osiris_key(cwd, "house").value


def read_seat_handle(cwd: str | None) -> str | None:
    """The handle of the seat this tree belongs to (TOML: seat = "..." in `.osiris`). A
    handle, not a seat:uuid, matches how the fleet already addresses seats everywhere (mail,
    fleet(), roster()); a rename drifts this the same way it drifts any handle-keyed
    reference, detectable and re-syncable by the migration verb, never a silent corruption.
    None means no seat declared (a bare code checkout nobody's office is)."""
    return _read_osiris_key(cwd, "seat").value


def read_tree_kind(cwd: str | None) -> str | None:
    """What kind of tree this is (TOML: kind = "..." in `.osiris`), one of office | worktree |
    repo | container. Before this key existed, nothing distinguished an office from a
    worktree from a plain repo from a container, so every consumer re-guessed from path
    shape. `container` is the data-level replacement for the hardcoded path-equality
    carve-outs (`offices.is_bare_office_root` et al.), read here but not yet consumed by them
    (that fold-in is separate, deliberate follow-up work, not this key's own landing). None
    means undeclared; callers keep whatever path-shape guess they used before this key
    existed."""
    return _read_osiris_key(cwd, "kind").value


def _write_model_pin_sync(office: Path, model: str) -> bool:
    """The synchronous half: keeps the operator's `/model` choice authoritative by
    automatically updating `.osiris`. Writes to the seat's own office specifically, never
    `identity.cwd` as given, which may be a code worktree or a repo root several
    directories up the climb that other seats' sessions also read: a pin write must never
    become a cross-seat side effect. `_scaffold_office`'s own convention (office/.osiris
    carrying project and model together) is preserved, not forked into a second file: reads
    `project` back out if a pin already exists so this never drops it, then rewrites both
    keys. Idempotent: returns False (nothing written) when the file already reads exactly
    this model, so an unchanged /model choice does not churn the disk on every subsequent
    mount.

    Never called with anything but a harness-observed model string (`SwapVerdict.to_model`,
    always a `deliberate`, a witnessed /model transition): a bare alias a human might type
    (e.g. "sonnet") never reaches this function, only what the harness itself reported
    running. Still refuses a value that cannot be a real model id (empty, or containing a
    quote/newline that would corrupt the TOML) as a defensive floor, never a validated
    allowlist; model ids change over time and this file has no business hard-coding them."""
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
            return False  # already correct, no churn
        project = existing.get("project")
    office.mkdir(parents=True, exist_ok=True)
    lines = ([f'project = "{project}"'] if project else []) + [f'model = "{model}"']
    pin.write_text("\n".join(lines) + "\n")
    return True


async def write_model_pin(seat_handle: str, model: str) -> bool:
    """The write side: update the seat's own `.osiris` pin so it becomes a cache of the
    operator's last /model decision rather than a competing, silently-stale claim. Before
    this, nothing in this codebase ever wrote the model pin; `.osiris`'s `model =` key was
    read at launch and hand-edited only. Runs on a thread, `_stamp_alive`'s own convention
    for filesystem I/O inside an async miner/handler."""
    import asyncio
    office = _default_office_root() / seat_handle.lower()
    return await asyncio.to_thread(_write_model_pin_sync, office, model)


def read_project_pin(cwd: str | None) -> OsirisKeyRead:
    """The full `project`-key read behind `read_project_label`: value plus, when a
    `.osiris` file exists but failed to parse/read, the path and error a banner can act on.
    `resolve_identity` uses this one, because it's the seam that carries the couldn't-read
    signal into `AgentIdentity` for mount()/orient() to confess. Everything else that only
    wants the plain fallback-to-basename value keeps using `read_project_label`, unchanged,
    still a bare `str | None`."""
    return _read_osiris_key(cwd, "project")


def project_pin_banner(ident: AgentIdentity) -> str | None:
    """Warns, never refuses: every directory that falls back to a basename guess is
    confessed, but mount() still succeeds. The same shape as the model-swap confession
    (`swaps.swap_banner`): loud, second-person, names exactly what's wrong, where, and the
    one fix that clears it, never a bare "cannot resolve". Silent for the bare seat-office
    root and when a real pin was found and used (the common, healthy case).

    Two messages now, not four: the other two ("no .osiris anywhere" and "found, valid,
    never declares `project`") are no longer errors at all. An unset project is a valid
    state (general-purpose, or not yet named), never a thing to alarm on. Those two moved
    to `project_pin_state` below, which mount() only renders after `self_heal_project_pin`
    has already tried and failed to fill it in from the graph's own unambiguous signal.
    What remains here are the two that stay genuine problems no self-heal can repair:
      CWD DOES NOT EXIST: the address itself is a ghost; no pin write repairs this, only
        reaping the graph's stale belief about it does.
      COULD NOT BE READ (broken TOML): fix the syntax error named in the message.

    The "silent for the bare root" claim above used to be only half true:
    `project_pin_missing` already excludes the bare root (resolve_identity's own
    `not bare_root` guard), but the third message above, "found, valid, never declares
    project", used to fire unconditionally whenever a real `.osiris` file exists with no
    `project` key, with no such carve-out. The container's own pin at
    ~/.osiris/seats/.osiris is exactly such a file: it deliberately declares
    `kind = "container"` and nothing else, not broken, not an oversight, the sanctioned
    non-project case. So the banner used to both misdiagnose a correct-by-design file as
    an error and name a mechanism ("fell back to a basename guess") that never ran here
    (the bare root's own project stays None, per resolve_identity's `bare_root` branch; by
    the time this banner would have rendered, `ident.project` is whatever a seated
    session's later house-resolution set it to, unrelated to any basename guess at all).
    A container-kind pin gets no banner: this is the healthy case, not a gap to warn
    about."""
    if is_bare_office_root(ident.cwd) or read_tree_kind(ident.cwd) == "container":
        return None
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
    return None


def project_pin_state(ident: AgentIdentity) -> str | None:
    """A project may not be decided right away: an unset project is valid, like a
    general-purpose agent or a project that isn't named yet, and the case should handle
    itself rather than alarm. These are the same two conditions `project_pin_banner` used
    to alarm on ("never declares `project`" / "no .osiris pin anywhere"), split out here
    because unset is a valid state, not an error: no warning symbol, no "fell back to a
    basename guess" framing (a guess implies something is owed and missing; an unset
    project is simply undecided, same as a fresh general-purpose seat). Silent for the two
    cases that remain real problems (`project_pin_cwd_missing`, `project_pin_error`); those
    still come from `project_pin_banner`, unchanged. mount() calls `self_heal_project_pin`
    first; this only ever renders when that mechanism found the graph's own signals
    ambiguous or absent and correctly declined to guess a value in."""
    if ident.project_pin_error or ident.project_pin_cwd_missing:
        return None  # project_pin_banner's own cases, a real problem, not a valid state
    if ident.project_pin_path:
        return (
            f"project: unset (general-purpose, or not yet named) — .osiris at "
            f"{ident.project_pin_path} is valid and answers a different question (e.g. only "
            "`model`), and does not declare `project`. This is a valid state, not an error. "
            "If you know your project, write it: correct_own_pin_value / write_pin_additions."
        )
    if ident.project_pin_missing:
        return (
            f"project: unset (general-purpose, or not yet named) — no .osiris pin exists "
            f"yet under {ident.cwd}. This is a valid state, not an error. If you know your "
            "project, write `.osiris` here with `project = \"...\"`, or pass it explicitly."
        )
    return None


def write_attribution_banner(ident: AgentIdentity) -> str | None:
    """Confessed, never acted on: if it picks, it is wrong, however good the pick. Warns
    when this lineage's own majority in_repo target disagrees with the project this
    session resolved; never overrides `project`.

    Stale-comparison guard, moved here to be testable the same way project_pin_banner is:
    `write_attribution_agreement` is stamped by register_agent, which runs before
    `_resolve_project_seat_first`, deliberately, by that function's own docstring, so the
    write gate still asserts a not-yet-seated session's fresh cwd-derived project
    unclobbered. For a seated session, that means the flag can be set against the
    pre-seat-override project (e.g. None, or a bare-root basename guess), while
    `ident.project` by the time this banner renders is already the post-override, final
    value, so the stored flag can say "disagrees" even though the two values this message
    would display are equal (both "osiris", the container root's own live specimen).
    Re-checking the live values here is the fix: the graph property still records the
    honest pre-override comparison (a real historical signal, untouched); this banner must
    never show itself agreeing with itself."""
    if ident.write_attribution_agreement != "disagrees":
        return None
    if ident.write_attribution_top == ident.project:
        return None
    return (
        f"⚠ this lineage's own writes mostly land in {ident.write_attribution_top!r} "
        f"({ident.write_attribution_total} in_repo edge(s) checked), but this session "
        f"resolved project={ident.project!r} — worth a look; nothing was overridden."
    )


def resolve_identity(
    *, cwd: str | None = None, job_dir: str | None = None,
    session: str | None = None, model: str | None = None, root: Path | None = None,
    claimed: set[str] | None = None, fallback_seed: str | None = None,
    project_label: str | None = None,
    store_reading: ModelReading | None = None,
) -> AgentIdentity:
    """Resolve an agent's identity from what it can tell the server plus what the harness
    records. The project comes from its cwd; the session and model are observed off its
    own record via the store (the sole lane since the JSONL-fallback removal): the caller
    feeds `store_reading` from transcript_store.identity_reading(), harness-agnostic, so
    non-Claude minds resolve exactly like Claude ones. No reading means no observation: the
    model honestly falls back to the agent's self-report. Observation outranks the agent's
    self-report (the harness doesn't lie; a swap is below the agent's own horizon), so a
    passed `model` is used only when nothing was observed, and a passed model that
    disagrees with the observation is kept as `model_declared` plus flagged
    `model_divergent`. `root` scopes the cwd sid-guess below (tests inject a tmp root;
    production reads ~/.claude/projects); the guess finds a session id, never a model.

    The claimed-sid guard: the cwd-locate grabs the hottest transcript's sid. Two
    concurrent same-project sessions without job_dirs would both grab the same one and
    merge. `claimed` (from the durable registry: sids already held by a live mount on
    another client session) makes the guess refuse a taken sid; the refuser falls to a
    deterministic per-client fallback keyed on `fallback_seed` (its MCP session key),
    distinct, stable across re-calls within the connection, and honestly resolved=False."""
    # the project label: an explicit override (env) beats the .osiris file, which beats the
    # folder basename, unless the folder is the bare seat-office root itself (the operator
    # launches agents from here on purpose, the intended pattern, not an accident). The
    # parent of every seat has no .osiris pin and no single project of its own; the basename
    # ("seats") would be a phantom, not a guess, so it stays unresolved from cwd. A
    # location-independent identity finds its project through its seat instead (mount()'s
    # seat-first resolution), never by inventing one from where it happens to be sitting.
    # An explicit project_label override short-circuits the cwd read entirely (unchanged
    # behavior): the couldn't-read signal only ever comes from an actual climb of cwd's own
    # .osiris file, never fabricated for an override that never touched one.
    pin_read = OsirisKeyRead(value=project_label) if project_label else read_project_pin(cwd)
    pinned = pin_read.value
    bare_root = is_bare_office_root(cwd)
    # the basename fallback climbs to the true repo root the same way the pin-lookup above
    # already does (_true_repo_root, same stopping rule): an unpinned repo worked from
    # inside a worktree (`<repo>/.claude/worktrees/<branch>`) must fall back to the repo's
    # own name, never the worktree's own throwaway branch-named directory.
    project = None if (pinned is None and bare_root) else (pinned or
             (_true_repo_root(cwd).name if cwd else None))
    # the third leg of the "why did this fall back to a basename guess" split: genuinely
    # nothing declared anywhere, as opposed to a broken file (pin_read.error) or a valid
    # file that just never sets `project` (pin_read.path with no error). Silent for the
    # bare seat-office root (its own carve-out) and when there is no cwd at all: neither is
    # a directory anyone could write a pin into. Also silent when cwd itself doesn't exist
    # (project_pin_cwd_missing below): a deleted office is not "missing a pin", it's not
    # there to pin at all; the two must stay disjoint, never folded into one flag (the
    # exact defect this fixes).
    pin_missing = (
        pinned is None and pin_read.error is None and pin_read.path is None
        and not bare_root and cwd is not None and not pin_read.cwd_missing
    )
    sid = session or _job_id(job_dir)
    confident = sid is not None  # a session/job_dir anchor; the cwd-locate below is only a guess
    declared = model  # the agent's self-report of its model (may be None), the weak signal
    observed: str | None = None
    observed_at: datetime | None = None  # when the record carrying the model was written
    method: str | None = None
    history: list[str] = []  # the transcript's model sequence, the swap history (job_dir path)
    deliberate = False       # a /model on the record makes any swap the operator's own hand
    # The store is the only observation lane since the JSONL-fallback removal. The
    # reading's own `method` is the harness name; identity's downstream contract (the seam
    # gates, _MODEL_EC) speaks the anchor vocabulary, so translate: an anchored discovery is
    # exactly what "job_dir" has always meant here (this session's own record, found by its
    # own anchor; the adapters enforce anchored_only just as the deleted probe did), and an
    # unanchored one is a hottest-guess that grades like the old cwd read (derived, never
    # seam-confessing). Without this translation a store reading graded CO_OCCURRENCE and
    # anchored=False, an under-grade the store-first mount path used to ship with.
    if store_reading and store_reading.current:
        observed = store_reading.current
        history = list(store_reading.history)
        deliberate = store_reading.deliberate
        observed_at = store_reading.observed_at
        method = "job_dir" if store_reading.anchored else "cwd"
        # an anchored reading may carry the sid; an unanchored one must never claim it:
        # adopting a hottest-guess sid as confident is the concurrent-session merge class
        if sid is None and store_reading.anchor_sid and store_reading.anchored:
            sid = store_reading.anchor_sid
            confident = True
    if sid is None and cwd:  # no anchor, so guess the session by cwd (sid only, never a model)
        path = locate_transcript_by_cwd(cwd, root=root)
        if path is not None:
            guess = path.stem.split("-")[0]  # the 8-char handle, matching the job-id scheme
            if claimed and guess in claimed:
                pass  # a live mount already holds this sid, refusing it beats merging into it
            else:
                sid = guess
    if observed is not None:                # the harness's word wins over the agent's own
        model = observed
        divergent = bool(declared and declared != observed)  # self-report != observation = flag
    else:                                   # nothing to observe, so fall back to the self-report
        model = declared
        method = "self_report" if declared else None
        divergent = False
    # A cwd-located id is the hottest transcript's: concurrent same-project sessions would
    # all grab it and silently merge, so only a session/job_dir anchor counts as resolved.
    # Marking the guess unresolved makes the fleet-digest health signal see it instead of
    # showing false-green.
    resolved = confident
    if sid is None:
        # Last resort: never collapse distinct sessions into one bucket, that is an
        # accidental identity merge (forbidden for Person; lossy to undo). Anchor on
        # whatever unique signal survives: the job_dir string is per-session even when its
        # id won't parse; else project-scope so cross-repo actors can't merge. A prior
        # shared `agent:unknown` sink was a conflation bug (a demotion can scramble
        # session-id resolution).
        if job_dir:
            sid = "j" + hashlib.sha1(job_dir.encode(), usedforsecurity=False).hexdigest()[:8]
        elif fallback_seed:
            # the claimed-sid refuser (or any anchorless client with a stable connection key):
            # deterministic per client session, distinct from every live claim, stable across
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


async def _flag_works_in_alongside_prior(
    actions: Actions, agent_oid: uuid.UUID, new_proj_oid: uuid.UUID, new_proj_label: str,
    src: str, now: datetime,
) -> None:
    """register_agent's own works_in write is add-only: `_link_once` never invalidates a
    prior edge to a different project, so a session that legitimately changes project
    across separate mounts accumulates live works_in edges forever. Measured rare before
    this was built (3 of 14 fleet-wide duplicate-works_in specimens, flat trend over 5
    weeks, zero covered by a declared multi-project charter), not common enough, and with
    no evidence source able to tell "changed project" from "works two projects", to
    justify auto-invalidating on write. This is the additive alternative: surface the
    moment it happens, never resolve it, a durable property naming both sides, for
    graph_lint (or a future reader) to grow a mechanical signal from, never a pick made
    here.

    Caller-gated to fire only the instant a genuinely new works_in edge is about to be
    created (an ordinary re-mount that finds its edge already live never reaches this), so
    it lands once per (agent, new project) pair, not once per mount."""
    prior = await actions.pool.fetch(
        "SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' AND l.to_id != $2 "
        "AND (l.valid_until IS NULL OR l.valid_until > now())",
        agent_oid, new_proj_oid)
    if not prior:
        return
    await actions.assert_property(
        agent_oid, "works_in_added_alongside_prior",
        {"new_project": new_proj_label,
         "prior_projects": sorted({r["canonical"].removeprefix("repo:") for r in prior}),
         "detected_at": now.isoformat()},
        src, now, _CONF, evidence_class=_EC)


async def _flag_unattributed_revisit(
    actions: Actions, *, base: str, project: str, src: str, now: datetime,
) -> None:
    """Operator ruling: most projects are unplanned or revisits, so a "revisit
    determinism" check is warranted. This checks whether a genuinely fresh Agent object
    just minted (register_agent's own `revisit_check`, gated to the one call site that
    actually needs it, mcp_server.py's `_reattach` transcript self-restore fallback: a
    real prior transcript proves the session ran before, but nothing links it to any
    known lineage, and it used to mint unconditionally with no check at all) is at a
    project that already carries `works_in` activity from some other, unrelated lineage.

    Never refuses the mint, the fresh identity is real, whatever route resolved it, only
    confesses that this may be one of four unresolved-revisit shapes (weeks-cold project,
    harness churn, no .osiris pin, foreign harness) landing as a stranger instead of a
    recognized return, exactly the population "zero inference-minted Agents after the
    fold" is meant to measure. `open_thread`'s own summary-hash dedup absorbs a repeat
    mount finding the same gap again, so this fires once per (agent, project) pair, not
    once per mount."""
    from src.orchestrator.capture import open_thread

    other = await actions.pool.fetchval(
        "SELECT o2.canonical FROM links l JOIN objects o ON o.id=l.to_id "
        "AND o.type='SoftwareProject' AND o.canonical=$1 "
        "JOIN objects o2 ON o2.id=l.from_id AND o2.type='Agent' AND o2.status='active' "
        "WHERE l.type='works_in' AND o2.canonical <> $2 AND o2.canonical NOT LIKE $2 || '-%' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "LIMIT 1",
        f"repo:{project}", base)
    if not other:
        return
    await open_thread(
        actions,
        f"UNATTRIBUTED REVISIT: a fresh identity ({base}) just minted at {project!r} via "
        "a transcript self-restore with no known lineage — the project already carries "
        f"works_in activity from {other}. Either a genuinely new visitor sharing this "
        "project, or one of the four unresolved-revisit shapes (weeks-cold project, "
        "harness churn, no .osiris pin, foreign harness) landing as a stranger instead "
        "of a recognized return. Never auto-resolved: a human's own judgment decides "
        "which.",
        kind="obligation", owner="operator", repo=project, source="revisit-determinism")


async def _succeeded_by_candidates(pool: asyncpg.Pool, canonical: str) -> list[str]:
    """Every distinct non-empty current `succeeded_by` value asserted on `canonical`,
    across every source, not just the single row an old linear walk's own `ORDER BY
    ... LIMIT 1` picked. `current_assertions` holds one current row per (object, name,
    source), so a node with more than one source ever asserting `succeeded_by` on it
    carries more than one simultaneously-current value: a genuine fork, not noise (one
    live specimen had six sources on a single node of one lineage). Grouped by value (the
    best-ranked row per value, same rank key an old query used: confidence, then
    observed_at, then assertion id, done in Python, not SQL, since the grouping itself is
    the point), then the distinct values are returned ordered by that same key descending;
    index 0 is exactly what the old single-path query would have picked alone, so a
    non-forked node (0 or 1 distinct values) behaves identically."""
    all_rows = await pool.fetch(
        "SELECT a.value #>> '{}' AS v, a.confidence, a.observed_at, a.id "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND o.type='Agent' AND a.name='succeeded_by'", canonical)
    best: dict[str, Any] = {}

    def _rank(r: Any) -> tuple[float, Any, int]:
        return (r["confidence"], r["observed_at"], r["id"])

    for r in all_rows:
        v = r["v"]
        if not v:
            continue
        if v not in best or _rank(r) > _rank(best[v]):
            best[v] = r
    ordered = sorted(best.values(), key=_rank, reverse=True)
    return [r["v"] for r in ordered]


async def _lineage_head_walk(
    pool: asyncpg.Pool, canonical: str, *, seen: set[str], budget: list[int],
) -> tuple[str | None, bool, int]:
    """Returns `(head, live, depth)` for the best branch reachable from `canonical`.
    `head` is the last node on that branch judged active and not false_mint (the same two
    checks `lineage_head`'s own `head` variable always required), never a dead/husk/
    merged terminal, or `None` when nothing valid was found anywhere in this subtree (a
    branch that dead-ends on a husk/merged node with no further succeeded_by at all).
    `None` is a first-class outcome, not a fallback string: a fully-dead branch must
    never out-compete a branch that found a real head purely by racking up more hops on
    the way to nothing (an earlier draft compared raw depth without this distinction and
    let a 1-hop dead end beat a 0-hop real head). `live` is whether that head is exactly
    live right now (`agent_liveness_exact`, never the lineage-base-widened
    `agent_liveness`; an unrelated live generation sharing the same base prefix must
    never make a stale fork branch read as live, that widening is exactly why a
    specific-generation caller has its own exact twin). `depth` is real hops travelled
    along the winning branch since this call (0 when a real head sits right here), the
    tenure signal, a longer-continuing branch over a shorter one, compared only among
    branches that both found a real head. `budget[0]` is a shared, mutable total-hop
    ceiling across the whole fork exploration (never per-branch), decremented once per
    edge taken anywhere in the recursion, so a wide fork can never cost more than an old
    single-path walk's own 64-hop bound already allowed.

    Liveness only decides a genuine fork, never an ordinary hop (caught live building
    this: an invariant test broke on the first draft, which let a live-but-unsucceeded
    base outrank its own unambiguous, single declared successor purely for not being
    currently mounted, exactly backwards for a function whose whole job is to follow the
    declared chain forward regardless of who is home). With exactly one candidate, this
    always continues into it unconditionally: `self_head` never enters a comparison at
    all, matching an old algorithm's own unconditional advance node for node.
    Liveness/tenure only arbitrate when there are two or more distinct candidates (a real
    fork); there, and only there, does stopping here (`self_head`) join the contest
    against the candidate branches."""
    from src.orchestrator.mounts import agent_liveness_exact

    row = await pool.fetchrow(
        "SELECT o.status='active' AS active, "
        " (SELECT ca.value #>> '{}' FROM current_assertions ca WHERE ca.object_id=o.id "
        "   AND ca.name='false_mint' ORDER BY ca.confidence DESC, ca.observed_at DESC "
        "   LIMIT 1) = 'true' AS false_mint "
        "FROM objects o WHERE o.canonical=$1 AND o.type='Agent'", canonical)
    self_head = canonical if (row and row["active"] and not row["false_mint"]) else None

    candidates: list[str] = []
    if budget[0] > 0:
        candidates = [c for c in await _succeeded_by_candidates(pool, canonical)
                     if c not in seen]

    if not candidates:
        if self_head is None:
            return None, False, 0
        live = (await agent_liveness_exact(pool, self_head)).get("live", False)
        return self_head, live, 0

    if len(candidates) == 1:
        # NOT a fork: the old algorithm's own unconditional advance, self_head is
        # irrelevant to a choice that was never there to make.
        nxt = candidates[0]
        seen.add(nxt)
        budget[0] -= 1
        head, live, depth = await _lineage_head_walk(pool, nxt, seen=seen, budget=budget)
        if head is not None:
            return head, live, depth + 1
        # the rest of the chain dead-ends with nothing valid at all, exactly the old
        # loop's own `if not nxt or nxt in seen: return head` shape, falling back to
        # whatever this hop's own self_head was (possibly still None, propagating on).
        if self_head is None:
            return None, False, 0
        live = (await agent_liveness_exact(pool, self_head)).get("live", False)
        return self_head, live, 0

    # a genuine fork (2+ distinct candidates): explore every branch, then self_head joins.
    branches: list[tuple[str, bool, int]] = []
    for nxt in candidates:
        seen.add(nxt)
        budget[0] -= 1
        head, live, depth = await _lineage_head_walk(pool, nxt, seen=seen, budget=budget)
        if head is not None:                # a fully-dead branch never enters the contest
            branches.append((head, live, depth + 1))
        if budget[0] <= 0:
            break
    if self_head is not None:
        self_live = (await agent_liveness_exact(pool, self_head)).get("live", False)
        branches.append((self_head, self_live, 0))

    if not branches:
        return None, False, 0           # nothing valid anywhere in this whole subtree

    # live wins first, then depth (tenure, the longer-continuing branch); ties keep
    # `branches`' own insertion order (Python sort is stable, and reverse=True never
    # reorders equal keys), which is candidate-rank order (an old single-path tie-break)
    # for the candidate-derived entries, self_head (depth 0) appended last, so a real
    # continuing branch only loses to "stop here" when strictly worse on both.
    branches.sort(key=lambda b: (b[1], b[2]), reverse=True)
    return branches[0]


async def lineage_head(pool: asyncpg.Pool, canonical: str) -> str:
    """Follow winning `succeeded_by` pointers to the newest active generation. A session-keyed
    resolve always lands on the base id (the transcript knows nothing of minting); the lineage
    decides who that name is now. Cycle-guarded; a missing object ends the walk. Pool-based so
    the liveness promotion (which has no Actions) can walk it too: a mount row must follow its
    lineage head, or a superseded generation reads as a live co-agent of its own descendant.

    Merged generations are not heads: a false successor folded away by the operator keeps
    its succeeded_by pointer on the record (append-only), so the walk still traverses it,
    but the head is the last generation still standing. Without this, every resolution
    walked back into the graveyard the merge had just closed. And a walk that starts on a
    merged node resolves through merged_into first: a row bound to a folded phantom must
    come home to the winner, not testify for the grave.

    A healed husk is also not a head: false_mint healing (heal.py / seam-debounce) never
    flips objects.status, so a husk stays 'active' forever, the same gap as retire_seat
    leaving Seat.status active, so a walk that landed on a husk as its final hop would
    wrongly call it the head. Walked live and confirmed this doesn't currently misroute
    anything (every husk checked still had its own real succeeded_by continuing the chain,
    so the walk already reached the true tail by just not stopping); this closes the
    latent edge case where a husk is the current tail (no real successor minted yet). Walk
    continuation is unchanged: `cur` still steps through a husk exactly as before, only
    the returned `head` now also requires false_mint absent.

    The true-tie bug: a phantom-fold heal's retraction (succeeded_by="") and the real
    succeeded_by re-assertion can land at the identical confidence and observed_at, both
    stamped by the same healing transaction's shared `now`. A retraction never
    legitimately outranks a same-instant real pointer, and since `_succeeded_by_candidates`
    drops every empty value outright, a retraction never even reaches the tie-break
    contest at all now.

    Stalling at a fork: a node can carry more than one simultaneously-current
    `succeeded_by` value, one per source, since `current_assertions` holds one current row
    per (object, name, source), and a messy multi-generation history (parallel compaction
    seams, healing events) can leave several sources each asserting a different successor.
    An old walk picked exactly one by rank order and continued blindly; if that pick
    dead-ended, the walk stopped at a stale, long-retired generation even though another
    candidate at that same fork led on to today's live head. Live specimen: three
    different generations of the same lineage all independently walked to the same
    six-way fork and stalled there instead of reaching the live head. Now
    `_lineage_head_walk` explores every distinct candidate at a fork (bounded by a shared
    64-edge budget across the whole exploration, matching the old per-path bound exactly)
    and the branch reaching a currently-live head wins, tie-broken by which branch
    travelled further (tenure), tie-broken again by the old rank order. A node with 0 or 1
    candidates at every hop, the overwhelmingly common case, walks node-for-node
    identically to the old algorithm."""
    cur = canonical
    for _ in range(10):
        winner = await pool.fetchval(
            "SELECT w.canonical FROM objects o JOIN objects w ON w.id=o.merged_into "
            "WHERE o.canonical=$1 AND o.type='Agent'", cur)
        if not winner:
            break
        cur = str(winner)
    canonical = cur
    head, _live, _depth = await _lineage_head_walk(
        pool, canonical, seen={canonical}, budget=[64])
    return head if head is not None else canonical


async def _succeeded_from_of(pool: asyncpg.Pool, canonical: str) -> str | None:
    """The immediate predecessor `canonical` succeeded, per its own `succeeded_from`
    property assertion, one hop. The shared primitive under both the ancestor-walk
    (nearest_handoff_ancestor, below) and lineage_root (ack_handoff's own lineage check),
    one mechanism, not two copies drifting."""
    val = await pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='succeeded_from' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canonical)
    return str(val) if val else None


async def lineage_root(
    pool: asyncpg.Pool, canonical: str, *, max_hops: int = 200,
) -> tuple[str, bool]:
    """The origin of this canonical's own succession chain, walked via succeeded_from
    edges, never a string parse: two generations of the same lineage share this root even
    when the id format itself changes across a renumbering.

    Returns `(root, complete)`, never a bare string. Found inside this function itself: a
    76-generation-deep lineage crossed an old max_hops=64 ceiling, and a bare-string
    return could not say so; it silently handed back whatever intermediate canonical the
    walk happened to reach at hop 64, confidently indistinguishable from a genuine origin.
    12 generations of that lineage each hit the ceiling at a different depth and resolved
    to 12 different wrong roots, reading as 12 real lineages until this was traced by hand
    past the bound. `complete=True` means the walk reached a genuine `succeeded_from IS
    NULL` terminus; `root` is trustworthy. `complete=False` means the walk exhausted
    `max_hops` before terminating; `root` is some real ancestor along the true chain, but
    not proven to be its origin, and a caller comparing two such roots for equality can be
    confidently wrong in both directions. Never treat a `complete=False` root as final;
    every caller below refuses rather than trusts one.

    Bounded like every other succeeded_from walk in this file (mint_heir, the ancestor-walk
    just below): a corrupt cycle can never hang this, but the bound is now a safety
    backstop, not the caller's only signal. `complete` is the fact to check, not silence
    on whether 64 (or 200, or any other number) was enough this time."""
    cur = canonical
    for _ in range(max_hops):
        nxt = await _succeeded_from_of(pool, cur)
        if nxt is None:
            return cur, True
        cur = nxt
    return cur, False


async def _lineage_ancestors(
    pool: asyncpg.Pool, canonical: str, *, max_hops: int = 200,
) -> tuple[list[str], bool]:
    """Self plus every real predecessor reached via succeeded_from, nearest first, plus
    whether the walk actually terminated: the full chain `lineage_root` walks to its end,
    exposed here so a caller can act on every generation along the way, not just the
    terminus. Returns `(ancestors, complete)`, same completeness contract as
    `lineage_root`: a caller must never read a short or clean-looking `ancestors` list as
    complete without checking the flag; `len(ancestors) - 1 >= max_hops` was the ad-hoc
    way `misfiled_by_lineage` used to re-derive this itself before `complete` became a
    first-class return value."""
    out = [canonical]
    cur = canonical
    for _ in range(max_hops):
        nxt = await _succeeded_from_of(pool, cur)
        if nxt is None:
            return out, True
        out.append(nxt)
        cur = nxt
    return out, False


async def misfiled_by_lineage(
    pool: asyncpg.Pool, agent_id: str, project: str | None, *, max_hops: int = 200,
) -> dict[str, Any] | None:
    """The discovery half: identity_coherence (settle.py's filed_under_check) only ever
    checks this session's own writes forward from its own mounted_at. It cannot help a
    later, correctly-filed successor find an earlier generation's misfiled writes, because
    nothing anywhere queries by lineage across projects; every read (orient/recall/search)
    is project-scoped, and a misfiled decision is invisible to that regardless of who's
    asking.

    Walks the caller's own succeeded_from chain (`_lineage_ancestors`, the third use of
    this session's shared primitive) and finds every Decision/Thread any generation of
    that lineage ever authored, anywhere in history, whose `in_repo` project disagrees
    with the caller's own current `project`.

    Report-only, never a gate or a repair, same law as filed_under_check: surfaces
    `misfiled`, never moves or corrects it. Repair is explicitly out of scope for this
    build.

    Always names what it cannot see: a succeeded_from chain that is broken partway up, or
    simply longer than `max_hops`, makes this under-report, never over-report; it can only
    find ancestors it can actually reach. `chain_hops_walked` and
    `chain_may_be_incomplete` are carried on every non-None result so a short or empty
    `misfiled` list is never indistinguishable from a chain that was never fully walked.
    `chain_may_be_incomplete` now comes directly from `_lineage_ancestors`'s own
    `complete` flag rather than being re-derived here from `len(ancestors) >= max_hops`;
    that re-derivation was this function's own correct workaround for a signal
    `_lineage_ancestors` didn't carry yet, and now that it does, trust the primitive
    instead of recomputing its answer. Returns None only in the one case this cannot
    evaluate at all (`project` unknown, mirroring filed_under_check) or the genuinely
    clean case (nothing misfiled and the chain terminated within `max_hops`, i.e. an
    answer this function actually stands behind); a clean answer earned by a complete
    walk is still allowed to render as silence, an incomplete walk never is, however clean
    it happens to look.

    Normalizes `project` through merged_into, the sibling gap to filed_under_check's own,
    same file this function mirrors: `filed_project` reads off live `in_repo` edges,
    already re-pointed to a fold's survivor by `_move_project_estate`, but the caller's
    own `project` may still name a label that has since been folded. Comparing raw
    strings then reports every one of that lineage's own correctly-filed writes as
    'misfiled' forever after the fold. Degrades to the raw label on any failure, same law
    as filed_under_check.

    Compounding gap, same as filed_under_check's own (caught by actually running a fold
    against this function): the `in_repo` join below carried no `valid_until` filter, so
    a folded project's invalidated pre-fold edge stayed visible alongside its live
    re-pointed replacement, reporting the same write as both correctly- and incorrectly-
    filed at once. Fixed in the same pass; label normalization alone cannot reconcile
    that, the edge set itself had to be live-only first."""
    if not project:
        return None
    try:
        from src.orchestrator.project_identity import _normalize_project_label_through_merge
        project, _confession = await _normalize_project_label_through_merge(pool, project)
    except Exception:  # noqa: BLE001, see note above
        pass
    ancestors, complete = await _lineage_ancestors(pool, agent_id, max_hops=max_hops)
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT o.id, p.canonical AS filed_project FROM objects o "
            "JOIN links l ON l.from_id = o.id AND l.type = 'in_repo' "
            "AND (l.valid_until IS NULL OR l.valid_until > now()) "
            "JOIN objects p ON p.id = l.to_id AND p.type = 'SoftwareProject' "
            "WHERE o.type IN ('Decision', 'Thread') AND EXISTS ("
            "  SELECT 1 FROM assertions a WHERE a.object_id = o.id "
            "  AND a.source_id = ANY($1::text[]) AND a.name = 'summary' "
            "  AND a.evidence_class = 'self_declared')",
            ancestors)
    except Exception:  # noqa: BLE001, fail open, same law as filed_under_check
        return None
    # The sibling gap filed_under_check already closed:
    # `_normalize_project_label_through_merge` above only ever matches an exact (or
    # case-variant) canonical, no name-property fallback. A `project` that's a bare
    # handle or a project's own `name` property post-rename (rename_project never
    # touches `canonical`, only `name`) stayed permanently "misfiled" against every one of
    # that lineage's genuinely-correct writes, purely because the label doesn't
    # string-match. Resolved the same way filed_under_check's own rescue is:
    # canonical-or-name-property, one real project, unambiguous, and, same fix as
    # filed_under_check's own, `project` itself is reassigned to the resolved canonical
    # (not just used to decide which rows still count as misfiled), so the receipt's own
    # `filed_under` names the same canonical the `misfiled` rows are compared against.
    filed_projects = {str(r["filed_project"]).removeprefix("repo:") for r in rows}
    if filed_projects and project not in filed_projects:
        try:
            from src.orchestrator.capture import _resolve_repo

            proj_id = await _resolve_repo(pool, project)
            if proj_id is not None:
                real_canon = await pool.fetchval(
                    "SELECT canonical FROM objects WHERE id=$1", proj_id)
                real = str(real_canon).removeprefix("repo:") if real_canon else None
                if real is not None and real in filed_projects:
                    project = real
        except Exception:  # noqa: BLE001, a diagnostic refinement must never be the
            pass          # reason this check goes blind, report-only, unchanged
    misfiled = sorted({
        (str(r["id"])[:8], str(r["filed_project"]).removeprefix("repo:"))
        for r in rows if str(r["filed_project"]).removeprefix("repo:") != project
    })
    hops_walked = len(ancestors) - 1
    incomplete = not complete
    if not misfiled and not incomplete:
        return None  # earned silence: nothing misfiled AND the chain was fully walked
    return {
        "filed_under": project,
        "misfiled": [{"id": i, "filed_under": p} for i, p in misfiled[:10]],
        "misfiled_count": len(misfiled),
        "chain_hops_walked": hops_walked,
        "chain_may_be_incomplete": incomplete,
    }


# The shared handoff-liveness predicate: a single boolean SQL expression over a bare
# `objects o` row, self-contained (no outer join a caller must already have set up) so
# both nearest_handoff_ancestor's set query (below) and ack_handoff's single-object check
# (mcp_server.py) resolve "is this a live handoff" the identical way. Structured first,
# prose as fallback, same law nearest_handoff_ancestor's own docstring already named: an
# explicit is_handoff='true' property wins; absent that property entirely, a
# self_declared summary mentioning "handoff"/"letter" counts too (legacy pre-property
# handoffs). Before this fix, ack_handoff had no fallback half at all, it only ever
# recognized the structured property, so a get_status() handoff_pending pointer produced
# purely by the prose fallback (no is_handoff property ever asserted on the object) could
# never be acknowledged: ack_handoff would always refuse it as "already acknowledged or
# is not a handoff", a permanently stuck pointer.
HANDOFF_LIVE_PREDICATE_SQL = (
    "(SELECT h.value #>> '{}' FROM current_assertions h "
    " WHERE h.object_id = o.id AND h.name = 'is_handoff' "
    " ORDER BY h.confidence DESC, h.observed_at DESC LIMIT 1) = 'true' "
    "OR ("
    "  NOT EXISTS (SELECT 1 FROM current_assertions h2 "
    "              WHERE h2.object_id = o.id AND h2.name = 'is_handoff') "
    "  AND EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id = o.id "
    "              AND s.name = 'summary' AND s.evidence_class = 'self_declared' "
    "              AND (s.value #>> '{}' ILIKE '%handoff%' "
    "                   OR s.value #>> '{}' ILIKE '%letter%'))"
    ")"
)


async def is_live_handoff(pool: asyncpg.Pool, object_id: Any) -> bool:
    """Single-object entry point onto `HANDOFF_LIVE_PREDICATE_SQL`, the same answer
    nearest_handoff_ancestor's own set query would give this one object, for a caller
    (ack_handoff) that already has a specific id in hand rather than a chain to walk."""
    return bool(await pool.fetchval(
        f"SELECT ({HANDOFF_LIVE_PREDICATE_SQL}) FROM objects o WHERE o.id = $1",
        object_id))


async def nearest_handoff_ancestor(
    pool: asyncpg.Pool, start_id: str, *, max_hops: int = 5, respect_ack: bool = True,
) -> tuple[tuple[str, list[dict[str, Any]]] | None, bool]:
    """Bounded chain-walk to the nearest ancestor bearing a handoff: a one-hop-only
    succession-note read goes blind the moment the immediate ancestor is a phantom (or
    simply never wrote a handoff) even though a real one sits one more hop back (a real
    repro: generation xxiv wrote a handoff, xxv was zero-turn and wrote nothing, xxvi
    arrived blind, one hop from xxv only). Shared by orient()'s succession-note block and
    the boot whisper's own succession-steering, one implementation, not two copies
    drifting.

    Structured first, prose as fallback: an is_handoff='true' property (ack_handoff's own
    typed stamp, once written) is the reliable half; the ILIKE '%handoff%'/'%letter%' text
    match stays for handoffs minted before that existed. Walks succeeded_from up to
    `max_hops` links (mint_heir's own kind of bound), returning the first ancestor found
    with a handoff-bearing Thread/Decision and its 2 freshest picks, or None if nothing is
    found within the bound (never widens into an unbounded search).

    `respect_ack` (default True): "is this baton still live", what orient()'s
    succession-note block and the boot whisper both actually want, from a "read receipt"
    redesign. An explicit is_handoff='false' (ack_handoff's own retirement stamp) excludes
    the record entirely, overriding the ILIKE fallback too: once acknowledged, a handoff
    must not resurrect for a later generation merely because nothing more recent exists;
    recall()/search() stay the route for that history, orient() should not re-deliver a
    baton someone already took. The fallback applies only to objects that never had an
    is_handoff property asserted at all (genuine pre-property legacy records); an object
    that has the property, however it currently resolves, is never routed through
    prose-matching. The property's current value is resolved the same way every other
    property-read in this codebase does (confidence DESC, observed_at DESC LIMIT 1), never
    a bare EXISTS(value='true'), which a superseding assertion from a different source
    (the acker, not the original author) would leave sitting in current_assertions as a
    non-winning but still-existing row.

    Pass `respect_ack=False` for a different question: "when did this reign end", a
    historical boundary fact that stays true whether or not anyone has since acknowledged
    reading it (`since_last_handoff`, handoff_compiler.py, is the one caller that wants
    this: it must keep finding its own already-acked handoff as its reign's own closing
    marker, or it would silently walk past it to a more distant ancestor and mis-date the
    boundary, the exact double-count bug its own docstring exists to prevent).

    Each returned pick now also carries `id` (the object's own short-resolvable uuid,
    stringified): ack_handoff needs a ref to acknowledge; before this fix callers had no
    way to name what they were looking at.

    Returns `(result, complete)`, the same completeness-signal shape
    `lineage_root`/`_lineage_ancestors` already carry (the third specimen of that exact
    disease this codebase has hit): `complete=True` when the walk reached a genuine
    stopping point within `max_hops`, either it found a handoff, or it walked all the way
    to a true `succeeded_from IS NULL` terminus and confirmed there is nothing to find.
    `complete=False` only when the walk exhausted `max_hops` without resolving either way:
    `result=None` is then not "nothing to inherit", it is "stopped looking", a collision
    once left unverified and later confirmed live: 64 of 636 real successions (10%) hit
    precisely this, a real marked handoff sits 6-13+ hops back, past `max_hops=5`, silently
    indistinguishable from a genuinely clean chain until now. `max_hops` itself is
    unchanged here on purpose: a bigger bound only moves the same silent cliff further
    out; a caller that needs to actually find a handoff beyond the default bound passes a
    larger `max_hops` explicitly and reads `complete` either way.
    This signal is built, not yet wired into any visible surface: no caller's own
    observable output changes in this pass; orient()'s succession_note, handshake's boot
    whisper, and handoff_briefing's boundary all still behave byte-for-byte as before for
    the same graph state. Surfacing `complete` to a reader is a deliberate, separate,
    operator-authorized step, held pending: it changes what every session sees on its
    first call, which is not this build's authorization."""
    ack_clause = HANDOFF_LIVE_PREDICATE_SQL if respect_ack else (
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
            return (cur, [dict(r) for r in picks]), True
        nxt = await _succeeded_from_of(pool, cur)
        if nxt is None:
            return None, True  # a genuine terminus reached, clean, not truncated
        cur = nxt
    return None, False  # max_hops exhausted, stopped looking, not "nothing to find"


def cap_handoff_text(text: str, limit: int = 800) -> str:
    """Truncate a handoff pick's `summary` for succession_note/boot-whisper display,
    marking it with '…' when actually shortened, the same never-silent-truncation law
    mcp_server.py's own `_cap_text` already enforces for open_threads/recent_decisions.
    An earlier inline `[:800]` at this call site predated that law and truncated real
    900-3500 char records with no marker, indistinguishable from a complete note."""
    return text if len(text) <= limit else text[:limit] + "…"


_LOCK_TIMEOUT = "5s"  # a genuinely wedged holder fails waiters loud, not silent


@asynccontextmanager
async def mint_lock(pool: asyncpg.Pool, lineage_root: str) -> AsyncIterator[None]:
    """Serialize generation-minting per lineage (a pg advisory lock on the root). Two
    concurrent seam observers once minted two generations in the same second with
    identical seam strings: each walked the head, each minted, and the loser's head-walk
    found the winner's fresh mint, so the race stacked generations instead of converging.
    The caller must re-read its evidence inside the lock so the loser sees the winner's
    write and concludes no-op.

    Transaction-scoped, after a fleet-wide wedge: pg_advisory_lock on a borrowed pool
    connection relied on a Python finally to unlock, and a cancelled/wedged coroutine
    could return the connection to the pool still holding it (advisory locks are
    session-scoped, unaffected by asyncpg's connection.reset()). pg_advisory_xact_lock
    inside an explicit transaction dies with the transaction instead, no finally needed,
    and a short SET LOCAL lock_timeout makes a genuinely wedged holder fail waiters loud."""
    from src.orchestrator.seats import LockWedged

    key = f"mint:{lineage_root}"
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
        try:
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", key)
        except asyncpg.exceptions.LockNotAvailableError as exc:
            raise LockWedged(
                f"mint_lock: {key!r} still held past {_LOCK_TIMEOUT} — another mint is "
                "genuinely in flight (or wedged)") from exc
        yield


# For the source_model property, the resolution method is the provenance, and observation
# outranks self-report for this substrate-fact: reading the model off the agent's own
# transcript (job_dir) is a DIRECT_OBSERVATION of the harness record; the cwd fallback is a
# weaker DERIVED guess (it may read a co-located session's transcript); a self-reported
# model is the agent's own word about its own substrate, the weakest signal (a swap is
# below its horizon), so it grades CO_OCCURRENCE, below both observations.
_MODEL_EC = {
    "job_dir": EvidenceClass.DIRECT_OBSERVATION,
    # a mounting sub-agent read off its own subagents/ transcript, as direct an
    # observation as job_dir, and it converges with the grade lineage.py stamps for the
    # same child. Not "job_dir", so the operator swap-detector (gated on job_dir) stays
    # quiet: a sub-agent legitimately running a different model is no rug-pull to confess.
    "subagent": EvidenceClass.DIRECT_OBSERVATION,
    "cwd": EvidenceClass.DERIVED,
    "self_report": EvidenceClass.CO_OCCURRENCE,
}


async def _last_anchored_stamp(
    actions: Actions, agent: uuid.UUID
) -> tuple[str | None, datetime | None]:
    """The last anchored source_model ever recorded for this Agent, and when it was
    observed: direct_observation grade only (a job_dir transcript probe), read off the raw
    assertions so a later weak-grade write from the same source can't hide it behind
    supersession. This is the succession baseline: only two anchored observations
    disagreeing can witness a seam; a cwd guess or self-report on either side would be a
    false alarm (one live agent's own falsely-flagged "demoted to haiku"). The timestamp
    is the seam gate's clock: only an observation fresher than this stamp may testify to a
    seam, the tail of a transcript is evidence about a past moment."""
    row = await actions.pool.fetchrow(
        "SELECT value #>> '{}' AS v, observed_at FROM assertions "
        "WHERE object_id=$1 AND name='source_model' AND evidence_class=$2 "
        "ORDER BY observed_at DESC, created_at DESC LIMIT 1",
        agent, EvidenceClass.DIRECT_OBSERVATION.value)
    if row is None:
        return None, None
    return row["v"], row["observed_at"]


_PHANTOM_FOLD_SRC = "phantom-fold"


_HALF_HEAL_SRC = "half-heal-detect"


async def _heal_completed(actions: Actions, grandancestor: str, phantom: str) -> bool:
    """Was `grandancestor`'s succeeded_by pointer actually unwound after `phantom` was
    flagged false_mint, or did the heal's multi-write sequence (flag stamps, pointer
    unwind, follow_binding, mount-row update, four separate unguarded writes) stop
    partway? A flag alone is not proof of completion: a real audit found three live
    specimens each stamped false_mint weeks earlier with the ancestor's succeeded_by
    still pointing straight at the phantom, the interruption permanently invisible to
    any reader that treats the flag itself as the answer."""
    current = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o "
        "ON o.id=a.object_id WHERE o.canonical=$1 AND o.type='Agent' "
        "AND a.name='succeeded_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        grandancestor)
    return bool(current != phantom)


async def _report_half_healed_phantom(
    actions: Actions, phantom: str, grandancestor: str,
) -> None:
    """A flagged-but-incomplete heal: false_mint landed, the pointer unwind never did.
    Never auto-completed here, on purpose: real generations may have been minted on top
    of the phantom since the interruption (exactly what happened to the three specimens
    this class was first found from), and finishing the unwind now would run
    follow_binding against whatever seat currently holds the live head, rebinding it
    backward onto a stale ancestor and stranding every real mind and every
    seat-addressed message since. Surfaced for a human's own judgment via the standard
    obligation call, idempotent on the summary so a repeat sighting (this walk runs at
    every mint) converges on one Thread rather than paging every caller who passes
    through here.

    Never re-opens a resolved thread: `open_thread` is idempotent on the summary hash, it
    finds the same Thread object whatever its current status and unconditionally
    re-asserts status='open', so a human's own resolve was being overridden by this
    detector's very next sweep, every 15 minutes, forever (a live check caught it: resolved
    seconds before, reopened seconds after). The condition being still present is real and
    worth saying, but re-opening a thread a human already closed is not this detector's
    call. If the thread already exists and currently reads status='resolved', this
    annotates it with the still-present sighting instead of calling open_thread at all.

    Still-open gets the same treatment, per an operator ruling to fix the sources: a
    thread already open from an earlier sweep needs no re-write either, kind/summary/
    owner/status were all identical every 15 minutes for the live specimen this was
    measured from (1,119 identical rows on one Thread). Only a genuinely new sighting (no
    Thread canon exists yet) calls open_thread at all now; every repeat sweep of an
    already-open or already-resolved Thread annotates instead, same no-write outcome,
    still records "still present"."""
    logger.warning(
        "half-healed phantom detected: %s (ancestor %s's succeeded_by never restored)",
        phantom, grandancestor)
    from src.orchestrator.capture import _thread_canon, annotate_thread, open_thread

    summary = (
        f"HALF-HEALED PHANTOM: {phantom} was flagged false_mint but its ancestor "
        f"{grandancestor}'s succeeded_by was never unwound — an interrupted heal "
        f"(decision ee012ebc). Do not auto-complete: a real successor may already be "
        f"live past {phantom}. A human must judge whether/how to reconcile.")
    canon = _thread_canon(summary, None)
    current_status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id WHERE o.canonical=$1 AND o.type='Thread' AND a.name='status' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canon)
    if current_status == "resolved":
        await annotate_thread(
            actions, canon,
            f"still present at {datetime.now(UTC).isoformat()}: {grandancestor}'s "
            f"succeeded_by remains unwound past {phantom}. Resolved once already — "
            "re-opening it is a human's call, not this detector's.",
            source=_HALF_HEAL_SRC)
        return
    if current_status is not None:
        # already open from an earlier sweep, nothing has changed, annotate rather than
        # reassert kind/summary/owner/status identically on every 15-minute tick
        await annotate_thread(
            actions, canon,
            f"still present at {datetime.now(UTC).isoformat()}: {grandancestor}'s "
            f"succeeded_by remains unwound past {phantom}. Already open from an earlier "
            "sweep.",
            source=_HALF_HEAL_SRC)
        return
    await open_thread(actions, summary, kind="obligation", owner="operator",
                      source=_HALF_HEAL_SRC)


_HALF_HEAL_BATCH_SRC = "half-heal-batch-repair"


async def correct_succession(
    actions: Actions, *, agent_id: str, value: str, because: str, actor: str,
    override_live: bool = False,
) -> dict[str, Any]:
    """The sanctioned entry point for `succeeded_by`: a batch of half-heal-detect threads were
    found genuinely live, not bulk-closeable, but bulk-reviewable, and no verb anywhere
    touched this property; a raw assert_property from outside the MCP surface is exactly
    what house policy rules against, and the auto-mode classifier caught it before it ran.
    Third-party, `rehold_seat`-shaped: gathers its own refusal evidence, writes once,
    receipts both sides of the change.

    `value=""` retracts the pointer to unset (the same NOT-NULL-safe empty-string
    sentinel `_debounce_roundtrip`/`_fold_zero_turn_ancestors` already use for a
    compensating retraction), never a delete; the stale assertion this supersedes stays
    in history, exactly the append-only law every other correction entry point in this house
    already holds to.

    The liveness guard mirrors `retire_agent`'s shape, not its exact check (this corrects
    a property on the named agent itself, not a seat's holder): `agent_id` reading live
    right now refuses by default. A mind still active is not settled history yet, and
    correcting its own succession record out from under it is exactly the "two signals
    disagree" shape this house's own population practice warns against.
    `override_live=True` names that as a deliberate act, the same escape hatch
    `retire_agent` already carries for the identical reason. But the check itself is
    `mounts.agent_liveness_exact`, never `retire_agent`'s own lineage-wide
    `agent_liveness`: a live specimen mid-batch caught the exact false positive
    `follow_binding`'s own guard was built to avoid. Every correction target here is, by
    the very nature of a half-heal repair, a historical ancestor whose lineage is very
    likely currently active. The widened check would read it "live" off its own
    descendant's fresh mount row and refuse nearly the whole batch, the opposite of what a
    "is this specific ancestor still active" question should ask.

    The receipt names both sides of the change and its own consequence, not just the
    write: `was`/`now` for the property itself, and `lineage_head` before and after the
    write for `agent_id`, the exact invariant the batch's own dry-run was built to verify
    per pair (a retraction that quietly moves the resolved head is the live-sibling-rebind
    class of bug this lane has been chasing; `head_moved` says so in the receipt itself
    rather than leaving a caller to re-derive it)."""
    because = (because or "").strip()
    if not because:
        return {"error": "because is required — correcting a succession pointer is a "
                         "deliberate act on the record"}
    agent_id = (agent_id or "").strip()
    row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
    if row is None:
        return {"error": f"no such agent: {agent_id!r}"}

    from src.orchestrator import mounts

    liveness = await mounts.agent_liveness_exact(actions.pool, agent_id)
    if liveness["live"] and not override_live:
        return {"error": f"{agent_id} is LIVE right now (last_seen {liveness['last_seen']}) "
                         "— correct_succession refuses to rewrite a live mind's own "
                         "succession record by default; pass override_live=True to "
                         "correct it anyway, a deliberate act on the record (mirroring "
                         "retire_agent's own override_live escape hatch)",
                "liveness": liveness}

    was = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='succeeded_by' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        row["id"])
    head_before = await lineage_head(actions.pool, agent_id)

    do = EvidenceClass.DIRECT_OBSERVATION
    now = datetime.now(UTC)
    await actions.assert_property(row["id"], "succeeded_by", value, _HALF_HEAL_BATCH_SRC,
                                  now, confidence_for(do), evidence_class=do.value, actor=actor)

    head_after = await lineage_head(actions.pool, agent_id)
    return {"agent_id": agent_id, "was": was, "now": value, "because": because,
            "was_live": liveness["live"], "head_before": head_before,
            "head_after": head_after, "head_moved": head_before != head_after}


async def is_occupied_by_a_live_body(
    pool: asyncpg.Pool, agent_id: str, *, agents_json: Any = None,
    read_exe: Any = None, read_cwd: Any = None,
) -> bool:
    """OCCUPANCY, never IDENTITY: does `agent_id`'s own `agent_mounts` row back a session
    the harness confirms is actually running right now (`registry_census`'s own `matched`
    set, where the harness roster and /proc both agree)? A generation can be minted and
    then phantom-folded a few seconds later, before its own first MCP write could
    register as an "act" - graph-silent but genuinely alive. `agent_has_acted` only ever
    asks whether this generation left a graph trace, so a generation this function
    returns True for is young, never a phantom, whatever its own graph activity looks
    like. It must be checked before `agent_has_acted` decides a candidate is foldable,
    never instead of it: a session that both acted and is occupied is doubly protected,
    not double-counted.

    Public (not `_`-prefixed) and pool-based, not Actions-based - read-only, since
    `mailbox.py`'s own send() eligibility gate needs this exact same check without
    wrapping a pool in an Actions object it never writes through.

    Injectable (`agents_json`/`read_exe`/`read_cwd`), same seam discipline as
    `registry_census` itself: the real defaults fire on every production mint, an
    accepted cost (a subprocess plus a bounded /proc walk) for a safety-critical check on
    a path that is not a hot loop.

    Extended for a non-Claude session beyond the Claude-only harness registry: the
    `matched` set can never confirm a non-Claude process by construction, since it shells
    out to `claude agents --json` and verifies the pid is the claude binary. Rather than
    teach `registry_census` to verify arbitrary foreign binaries via /proc (fragile,
    one-off per harness), a session the census structurally cannot see gets its own path:
    `registry_census`'s `pulse_live` (a self-reported freshness, 5-minute window, stricter
    than the Claude path's 15-minute mount-staleness window this function's own callers
    gate liveness with, since a self-report is weaker evidence than a verified census
    match). A stale non-Claude seat (no pulse within 5 minutes) reads cold exactly like a
    stale Claude one: this is a genuine freshness check, not a blanket exemption for
    anything non-Claude. The Claude path (`matched`) is entirely unchanged."""
    from src.orchestrator.mounts import registry_census

    census = await registry_census(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    if agent_id in {m.get("agent_id") for m in census.get("matched", [])}:
        return True
    return agent_id in {m.get("agent_id") for m in census.get("pulse_live", [])}


async def _fold_zero_turn_ancestors(
    actions: Actions, ancestor_id: str, ancestor_oid: uuid.UUID, now: datetime, *,
    agents_json: Any = None, read_exe: Any = None, read_cwd: Any = None,
) -> tuple[str, uuid.UUID]:
    """SUCCESSION FOLLOWS TURNS, NOT HARNESS EVENTS: a generation claiming to be a
    predecessor that never had any turns is not a predecessor - a generation minted but
    never acted upon must never appear as a link in the inheritance chain, count a reign
    numeral, or intercept a handoff. Canonical repro: /compact then /model back-to-back
    with zero turns between minted two generations (a phantom, then its heir) for one
    real seam.

    Called by both real mint call sites (live_succession, register_agent) right before
    they call mint_heir, not from inside mint_heir itself, since mint_heir's return tuple
    is unpacked by roughly 20 test call sites and threading the resolved ancestor back
    out would mean touching every one of them for a fact the caller already has before it
    calls in. Two call sites is a tractable, by-hand audit surface (grep mint_heir\\( in
    src/ to verify - there are exactly two).

    Extends the existing mint gate rather than adding a new one beside it: an earlier fix
    (which built _debounce_roundtrip) coalesces duplicate observations of one real seam
    event (two observers racing the same /model). This is the other residual class: two
    real, different seam events back-to-back (compact, then swap) with no turns between.
    The outcome has to differ, deliberately - a round-trip returns to a value this
    lineage already had, so nothing new ever happened and _debounce_roundtrip heals to no
    mint at all; a compact-then-swap reaches a genuinely new model, which still deserves
    a numeral - coalescing here means mint once, not mint zero, so this folds the phantom
    and lets the caller's normal mint_heir call proceed against the corrected ancestor,
    rather than returning a heal dict that skips minting the way _debounce_roundtrip's
    own round-trip case correctly does. What is shared, on purpose: the same window
    (_SEAM_DEBOUNCE_SECS, the mint gate's actless-head window, not a second one), the
    same acts-check (agent_has_acted), and the same false_mint/retired stamp shape.

    Walks up through any consecutive run of zero-turn ancestors within that window (the
    same 64-iteration bound mint_heir's own grave-avoidance loop uses), un-minting each,
    until the chain lands on either a real (witnessed) ancestor, the lineage root, or a
    hop outside the window. A root (no succeeded_from of its own, nothing minted it) is
    never folded; it has nothing to fold into. Idempotent: an already-folded phantom
    (false_mint already true and the ancestor's succeeded_by pointer actually unwound)
    halts immediately, unchanged - safe to re-run the fleet sweep below as often as
    wanted. A phantom flagged false_mint without the pointer unwind (an interrupted heal)
    is not treated as done: it is reported via _report_half_healed_phantom and the walk
    still halts there, but never silently, and never auto-completed (see that helper's
    own docstring for why finishing it here would be unsafe, not merely overdue).

    Occupancy is checked before every fold: `is_occupied_by_a_live_body` runs alongside
    `agent_has_acted` - a candidate generation whose own `agent_mounts` row backs a
    harness-confirmed live session is never folded, whatever its graph activity says. A
    mint-then-immediate-fold sequence (this walk's own second path, or the 15-minute
    sweep) used to have only `agent_has_acted` to ask, and a session a few seconds old has
    usually made no graph write of its own yet - graph-silent is not the same fact as
    dead."""
    cur_id, cur_oid = ancestor_id, ancestor_oid
    for _ in range(64):
        meta = {r["name"]: (r["v"], r["at"]) for r in await actions.pool.fetch(
            "SELECT DISTINCT ON (name) name, value #>> '{}' AS v, observed_at AS at "
            "FROM current_assertions WHERE object_id=$1 "
            "AND name IN ('succeeded_from', 'minted_because', 'false_mint') "
            "ORDER BY name, confidence DESC, observed_at DESC", cur_oid)}
        if meta.get("false_mint", (None, None))[0] == "true":
            grandancestor, _ = meta.get("succeeded_from", (None, None))
            if grandancestor and not await _heal_completed(actions, grandancestor, cur_id):
                await _report_half_healed_phantom(actions, cur_id, grandancestor)
            break  # a flagged row's own branch stops here either way: complete or
                   # half, this walk never proceeds past it
        if "minted_because" not in meta:
            break  # a root, nothing minted it, nothing to fold
        grandancestor, _ = meta.get("succeeded_from", (None, None))
        minted_at = meta["minted_because"][1]
        if not grandancestor:
            break
        if minted_at is None or (now - minted_at).total_seconds() > _SEAM_DEBOUNCE_SECS:
            break  # outside the mint gate's own window, too old to call "back-to-back"
        grand_oid = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", grandancestor)
        if grand_oid is None:
            break
        if await agent_has_acted(actions, cur_id, exclude=[cur_oid, grand_oid],
                                 settled_after=minted_at):
            break  # a real mind lived here, nothing to fold
        if await is_occupied_by_a_live_body(
            actions.pool, cur_id, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd,
        ):
            break  # a harness-confirmed live session sits here, young, not a phantom
        do = EvidenceClass.DIRECT_OBSERVATION
        conf = confidence_for(do)
        # ATOMIC: these four writes either all land or none do. Before this fix they were
        # four independent unguarded statements, the same half-heal shape an earlier
        # interruption in _debounce_roundtrip produced, just with a smaller blast radius
        # (this walk only ever touches the not-yet-superseded head, so an interruption
        # here can't produce the dangerous stale-ancestor-rebind shape a later
        # interruption elsewhere could), but the class is worth closing everywhere it
        # appears, not only where it was first found.
        from src.orchestrator.seats import follow_binding
        async with actions.atomic() as a:
            for k, v in (("false_mint", True), ("retired", True),
                         ("retired_by", _PHANTOM_FOLD_SRC),
                         ("false_mint_because",
                          "zero-turn generation folded at supersession (ruling d3531cd8) — "
                          "minted but never acted upon before the next seam")):
                await a.assert_property(cur_oid, k, v, _PHANTOM_FOLD_SRC, now, conf,
                                        evidence_class=do.value)
            await a.assert_property(grand_oid, "succeeded_by", "", _PHANTOM_FOLD_SRC, now,
                                    conf, evidence_class=do.value)
            await a.execute(
                "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
                grandancestor, cur_id)
            # THE SAME ESTATE-TRANSFER RULE mint_heir HOLDS: this walk is a second
            # mail-reassignment site, mint_heir's own sibling, and it needs to do both
            # halves of that transfer, not just the fleet_messages half above. A phantom
            # generation folded here could carry a lease (message_recipients row, read_at
            # IS NULL, meaning the phantom itself read a message but never settled it
            # before being folded) that then sat orphaned on the now-retired phantom's own
            # id, invisible to the grandancestor it was folded into. Same fix, same
            # filter: only a genuinely settled row (read_at IS NOT NULL, real memory, not
            # a lease) carries onto the grandancestor; a bare lease carries nothing, so
            # the grandancestor's own next inbox() finds that message fresh, never
            # silently vanished, never falsely pre-settled.
            await a.execute(
                "INSERT INTO message_recipients "
                "(message_id, agent_id, delivered_at, read_at, deliveries) "
                "SELECT message_id, $1, delivered_at, read_at, deliveries "
                "FROM message_recipients WHERE agent_id=$2 AND read_at IS NOT NULL "
                "ON CONFLICT (message_id, agent_id) DO NOTHING",
                grandancestor, cur_id)
            await follow_binding(a, ancestor_oid=cur_oid, heir=grandancestor,
                                 heir_oid=grand_oid, now=now)
        cur_id, cur_oid = grandancestor, grand_oid
    return cur_id, cur_oid


async def _skip_false_mint_ancestors(
    actions: Actions, agent_id: str, oid: uuid.UUID,
) -> tuple[str, uuid.UUID]:
    """THE REANIMATION FIX: `register_agent`'s reanimation-of-retired path hands
    `_fold_zero_turn_ancestors` the retired object itself as the presumed ancestor to
    fold from - but that function's own contract treats an already false_mint starting
    node as a terminal halt (correct mid-walk, where it means "a prior fold already
    resolved this and I should stop here"), so it returned the phantom completely
    unchanged, and the reanimated heir's `succeeded_from` chained onto a folded phantom
    instead of a real mind.

    Walks forward past any consecutive false_mint ancestors via their own
    `succeeded_from`, landing on the first eligible (non-false_mint) ancestor, or the
    lineage root if the whole visible chain is false_mint. A no-op when `agent_id` itself
    isn't false_mint - every other mint path (this is called only from the reanimation
    arm) is unaffected. Same 64-hop bound as every other succeeded_from walk in this
    file."""
    cur_id, cur_oid = agent_id, oid
    for _ in range(64):
        flagged = await actions.pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
            "AND name='false_mint' ORDER BY confidence DESC, observed_at DESC LIMIT 1",
            cur_oid)
        if str(flagged).lower() != "true":
            return cur_id, cur_oid
        pred = await actions.pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
            "AND name='succeeded_from' ORDER BY confidence DESC, observed_at DESC LIMIT 1",
            cur_oid)
        if not pred:
            return cur_id, cur_oid  # a false-minted root, nothing eligible above it
        pred_oid = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", pred)
        if pred_oid is None:
            return cur_id, cur_oid
        cur_id, cur_oid = pred, pred_oid
    return cur_id, cur_oid


async def reinstate_generation(
    actions: Actions, agent_id: str, *, because: str, actor: str,
) -> dict[str, Any]:
    """THE FOLD'S INVERSE: `_fold_zero_turn_ancestors` had no undo before this was added;
    this is it, built for the case where a genuinely live generation was wrongly
    phantom-folded.

    Retracts `false_mint`/`retired` on `agent_id` (a fresh assertion of `false` on each,
    the same append-only rule every property in this codebase follows, never a rewrite of
    the fold's own record, which stays walkable) and re-links `succeeded_from` to the
    nearest eligible (non-false_mint) ancestor in its own chain, walking past any
    ancestor that was itself correctly, separately folded (never blindly restoring the
    immediate predecessor, which may still be a legitimate phantom this call has no
    business un-folding). Moves the seat binding, any active `holds` link anywhere in
    this lineage, onto `agent_id`, the exact inverse of the fold's own `follow_binding`
    call, so a seat-addressed message reaches the reinstated generation immediately.

    Result-verified: returns exactly what changed (`{"false_mint": {"old","new"}, ...}`),
    never a bare success flag - a repair call with no visible effect is worse than one
    that refuses outright.

    Refuses, writes nothing, when `agent_id` names no known Agent object, or when it is
    not currently false_mint/retired at all - reinstating a generation that was never
    folded is not this function's job, and a caller asking for it almost certainly named
    the wrong id."""
    oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", agent_id)
    if oid is None:
        return {"ok": False, "detail": f"{agent_id!r} is not a known Agent object"}
    was_false_mint = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='false_mint' ORDER BY confidence DESC, observed_at DESC LIMIT 1", oid)
    was_retired = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='retired' ORDER BY confidence DESC, observed_at DESC LIMIT 1", oid)
    if str(was_false_mint).lower() != "true" and str(was_retired).lower() != "true":
        return {"ok": False,
                "detail": f"{agent_id!r} is not currently false_mint or retired — "
                          "nothing to reinstate"}
    old_predecessor = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='succeeded_from' ORDER BY confidence DESC, observed_at DESC LIMIT 1", oid)
    # Walk past any consecutive false_mint ancestor to the nearest eligible one, the same
    # walk _skip_false_mint_ancestors does for the reanimation path, applied here to the
    # predecessor chain instead of the entry id (this function's own `agent_id` is the
    # one being un-folded, not the one being skipped).
    eligible, eligible_oid = old_predecessor, None
    hops = 0
    while eligible and hops < 64:
        e_oid = await actions.pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", eligible)
        if e_oid is None:
            eligible = None
            break
        fm = await actions.pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
            "AND name='false_mint' ORDER BY confidence DESC, observed_at DESC LIMIT 1", e_oid)
        if str(fm).lower() != "true":
            eligible_oid = e_oid
            break
        nxt = await actions.pool.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
            "AND name='succeeded_from' ORDER BY confidence DESC, observed_at DESC LIMIT 1",
            e_oid)
        eligible = nxt
        hops += 1

    now = datetime.now(UTC)
    do = EvidenceClass.DIRECT_OBSERVATION
    conf = confidence_for(do)
    changed: dict[str, Any] = {
        "false_mint": {"old": was_false_mint, "new": "false"},
        "retired": {"old": was_retired, "new": "false"},
    }
    from src.orchestrator.seats import follow_binding
    async with actions.atomic() as a:
        await a.assert_property(oid, "false_mint", False, actor, now, conf,
                                evidence_class=do.value)
        await a.assert_property(oid, "retired", False, actor, now, conf,
                                evidence_class=do.value)
        await a.assert_property(oid, "reinstated_because", because, actor, now, conf,
                                evidence_class=do.value)
        if eligible and eligible != old_predecessor:
            await a.assert_property(oid, "succeeded_from", eligible, actor, now, conf,
                                    evidence_class=do.value)
            if eligible_oid is not None:
                await a.assert_property(eligible_oid, "succeeded_by", agent_id, actor, now,
                                        conf, evidence_class=do.value)
            changed["succeeded_from"] = {"old": old_predecessor, "new": eligible}
        await follow_binding(a, ancestor_oid=oid, heir=agent_id, heir_oid=oid, now=now)
    return {"ok": True, "agent_id": agent_id, "because": because, "changed": changed}


_AGENT_PROJECT_LINK_TYPES = ("works_in", "governs")


async def move_agent_project_links(
    actions: Actions, from_oid: uuid.UUID, to_oid: uuid.UUID, actor: str, now: datetime,
) -> dict[str, int]:
    """Re-point every live works_in/governs edge from `from_oid` onto `to_oid`: invalidate
    plus create, the same pattern `_move_project_estate`/`_move_agent_estate`/`_move_seat_
    estate` already use (measured 906 of 6,245 fleet-wide), applied here to an agent's
    own outbound project edges instead of a project's inbound ones. Two callers, one
    implementation, to avoid writing a fourth estate-mover: `mint_heir` (prospective, an
    ancestor's edges move to its fresh heir on ordinary succession, the mechanism that was
    missing entirely) and `folds._move_agent_estate` (a different, related gap:
    fold_agent's own estate-move never covered works_in/governs at all, only
    mail/mounts/threads; reconcile_agent_fold inherits the fix automatically since it
    calls the same function unchanged).

    Idempotent (a link already live on `to_oid` is never duplicated) and history-
    preserving: the invalidated link's row stays exactly where it was, in whose name and
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
    """THE ONE-TIME REPAIR for a previously measured leak (906 of 6,245 live
    works_in/governs edges fleet-wide, across roughly 400 lineages): the write-side fixes
    (`mint_heir`, `folds._move_agent_estate`) stop it from growing further but never touch
    what already exists. Every Agent whose canonical is not its lineage's current
    `living_head` but still carries a live works_in/governs edge gets that edge moved onto
    the head, via the same `move_agent_project_links` both write-side fixes already use,
    not a third implementation of the move itself, only a new enumeration of who needs
    it.

    Dry run is the default (`dry_run=True`, mirroring `backfill_unbound_seats`'s own
    established shape): reports the plan (how many edges each off-head agent would give
    up, and which living head each resolves to) without writing anything. `only_bases`
    scopes both the plan and the write to exactly those lineage bases (staged rollout,
    same convention `backfill_unbound_seats`'s `only_seats` already uses); every other
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
    """A head drops one of its own duplicate works_in edges, closing a toolkit gap:
    `unpeer` heals peer_of, `detach_seat` heals managed_by, but nothing healed works_in
    before this, so a live agent carrying two simultaneously-live works_in edges (one
    specimen: an agent pointing at two projects, both self-declared, one the stale side
    of an earlier fork) had no repair path except raw SQL. orient() resolves through
    whichever edge wins, so the duplicate is not cosmetic; it can hide a lineage's own
    threads/decisions from itself, live.

    Same posture as correct_house: self-scoped identity hygiene, never operator-fenced,
    but the self-scoping lives entirely in the MCP wrapper's refusal to expose `agent_id`
    as a parameter (auto-filled from the caller's own resolved identity), exactly as
    correct_house's own underlying function takes an explicit `agent_id` and does not
    itself check agent_id==actor. This function stays generic/composable on purpose (the
    same shape backfill_agent_project_links needs for a future scripted sweep).

    Deliberately narrow: a same-agent, same-generation cleanup, orthogonal to the
    still-open question of whether a predecessor generation's stale works_in edge gets
    moved on succession, write-side or read-side. This never touches an ancestor's edges,
    never re-points anything onto a different agent, and does not use the estate-move
    pattern `_move_agent_estate`/`move_agent_project_links` use for exactly that reason:
    those move edges between two agent objects; this invalidates one of the same agent's
    own two edges. No mail/mounts/thread-ownership moves with it (unlike a fold's estate
    move) because nothing there is project-scoped in a way a dropped works_in edge would
    orphan.

    Refuses loudly on: blank `because`; `agent_id` not resolving to an active Agent;
    `stale_project` resolving ambiguously (never guesses) or to no SoftwareProject at
    all; no active works_in edge from `agent_id` to it; or `stale_project` naming the
    agent's only live works_in edge. Dropping the last project is not cleanup, it is
    amputation; this function exists for duplicates, never for a lone edge."""
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


async def retire_governs_edges(
    actions: Actions, agent_id: str, repos: list[str], *, because: str, actor: str,
) -> dict[str, Any]:
    """Drop one or more of a third-party agent's own `governs` edges, closing a toolkit
    gap: a stale/off-head Agent generation can carry live governs edges nobody wants
    moved forward (`backfill_agent_project_links`'s own move-onto-the-living-head repair
    is the wrong shape for garbage; it would re-pollute a clean current head with exactly
    what this retires instead), and `set_charter`/`charter_for` cannot see them at all
    (seat-origin only, by design: `invalidate_link(seat_oid, ...)`, never an Agent
    object). This is the missing per-edge retire, same generic/composable posture as
    `invalidate_works_in` right above: never moves anything, never guesses which edges
    are garbage. The caller names them, one call retires a whole batch under one shared
    `because`, and each edge gets its own compensating `invalidate_link` event.

    Refuses loudly on: blank `because`; `agent_id` not resolving to an active Agent; an
    empty `repos` list. Per-repo, this never aborts the whole batch on one bad name: a
    `repos` entry that doesn't resolve to a known SoftwareProject, or resolves but the
    agent carries no live `governs` edge to it, is reported in `not_found`/`no_edge`
    rather than raising, so the caller sees exactly what happened to every name it gave,
    the same discipline `set_charter`'s own `rejected` list already establishes."""
    from src.orchestrator.projects import _resolve_project_ref

    because = (because or "").strip()
    if not because:
        return {"error": "because is required — retiring a governs edge is a deliberate "
                         "act on the record"}
    agent_id = (agent_id or "").strip()
    if not agent_id:
        return {"error": "agent_id is required"}
    agent_row = await actions.pool.fetchrow(
        "SELECT id, canonical FROM objects WHERE canonical=$1 AND type='Agent' "
        "AND status='active'", agent_id)
    if agent_row is None:
        return {"error": f"no such active Agent: {agent_id!r}"}
    repos = [r.strip() for r in (repos or []) if r and r.strip()]
    if not repos:
        return {"error": "repos is required — at least one project name to retire"}

    live = await actions.pool.fetch(
        "SELECT to_id, t.canonical AS project FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='governs' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_row["id"])
    live_by_id = {r["to_id"]: r["project"] for r in live}

    now = datetime.now(UTC)
    retired: list[str] = []
    retired_ids: set[Any] = set()
    not_found: list[str] = []
    no_edge: list[str] = []
    for name in repos:
        proj_row, err = await _resolve_project_ref(actions.pool, name, verb="retire_governs")
        if err or proj_row is None:
            not_found.append(name)
            continue
        if proj_row["id"] not in live_by_id:
            no_edge.append(name)
            continue
        await actions.invalidate_link(agent_row["id"], proj_row["id"], "governs", actor, now)
        retired.append(str(proj_row["canonical"]))
        retired_ids.add(proj_row["id"])
    if retired:
        await actions.assert_property(agent_row["id"], "governs_retired_because", because,
                                      actor, now, _CONF, evidence_class=_EC)
    remaining = sorted(v for k, v in live_by_id.items() if k not in retired_ids)
    return {"agent": agent_row["canonical"], "retired": retired, "not_found": not_found,
            "no_edge": no_edge, "still_governs": remaining, "because": because}


async def _resolve_or_mint_project(actions: Actions, project: str, actor: str) -> uuid.UUID | None:
    """Find-or-create a SoftwareProject case-insensitively on its bare label: both
    mint_heir and register_agent used to call `create_or_find_object("SoftwareProject",
    f"repo:{label}", ...)` directly, a literal, case-sensitive canonical lookup, so a
    case-differing pin (e.g. "xxit" vs an upstream "Xxit", or "RAMstein" vs "ramstein")
    did not compete over the `name` property, it minted a whole separate object. Measured
    live: one project carried exactly this duplicate, repo:RAMstein (an early pin, not
    even a git checkout) alongside repo:ramstein (a real git repo, remote-verified), out
    of 81 active+retired SoftwareProjects fleet-wide, exactly one duplicate group.

    Never lowercase-normalizes: a mixed-case name like "Like-Us" can be genuine upstream
    truth (the git remote itself is mixed-case), so folding every match onto one
    canonical casing would be exactly the wrong fix; this only finds an existing object
    regardless of case, it never rewrites which case wins.

    Exactly one case-insensitive match: reuse it. A genuinely new project is never
    blocked (zero matches falls through to the ordinary literal create). Two or more
    existing matches (a pre-existing duplicate): not this function's call to arbitrate
    which one is "real" - that is fold_project's deliberate, evidence-gated job, not a
    mint-time guess. Falls through to the literal, unchanged lookup so an already-
    ambiguous population is never silently collapsed onto a random pick.

    Refuses to mint (or reuse) a degenerate bare label (e.g. repo:? with name='?', no
    commits, no genuine identity): capture.py's `_validate_repo_name` was meant to be the
    single choke point for every legitimate SoftwareProject mint, but was never actually
    wired into this path (nor mint_heir's, ingest_files', bootstrap's, or
    correct_project_name's own mints) - most live mint sites bypassed it entirely, which
    is exactly how a bare "?" got through. All mint sites are now covered: ingest_files
    (src/ingest/files.py) and bootstrap (bootstrap.py) each now refuse loudly (return
    `{"error": ...}`) before minting; correct_project_name (projects.py) validates the
    majority-vote `settled` name it's about to bless as canonical, even though it mints
    no new object. Reuses that same regex here rather than inventing a second, possibly-
    diverging definition. Returns None rather than raising: this runs deep inside
    ordinary mount/succession traffic, where a caller-side exception would be a much
    louder failure than a malformed label deserves - the caller simply has nothing to
    link works_in to this turn, same as an honestly-None project already does. Existing
    degenerate objects (repo:? itself) are never reused either - a caller landing here
    with the same garbage label a second time must not keep growing its edge count.

    Refuses the operator's own sentinel names (e.g. repo:operator, measured live: the
    human's desk address, never a real repository, minted as a SoftwareProject because
    `_REPO_NAME_RE` happily matches the bare word "operator"). This is the choke point
    for every project label an agent's own mount/succession ever mints through (mount()'s
    register_agent via `identity.project`, mint_heir via `heir_project`); both call sites
    pass whatever `project`/`house` basename-guess or pin resolved to, with no upstream
    filter for the house's own non-project sentinels. Excludes the whole
    `_OPERATOR_ACTORS` set (seats.py), not just the literal "operator", for the same
    reason `_seat_lineage_ancestor` already does - "analyst:operator" and "console" are
    exactly as much "not a repository" as "operator" is.

    The rename stub (live specimen: a project renamed from "xxit" to "handlingtheloop",
    where the canonical stays repo:xxit forever and only the `name` property changes, per
    rename_project's own rule): a canonical-only lookup goes blind to that new name the
    instant it's declared, so the very next mount whose pin already reads the new label
    finds no canonical match and mints a fresh object under it - nine live works_in edges
    landed on the stub before this was caught. `_resolve_software_project` (projects.py)
    already treats canonical-string and winning-`name`-property matches as one
    inseparable label lookup for every other project function; this mint-time choke
    point never got the same treatment. Checked only when the canonical search comes up
    empty (a canonical hit, even an ambiguous case-differing one, is unchanged; this only
    closes the zero-match gap a rename opens), and only trusted on an unambiguous single
    name match; two or more, exactly like two or more canonical matches above, is not
    this function's call to arbitrate and falls through to the literal mint-or-find."""
    from src.orchestrator.capture import _REPO_NAME_RE
    from src.orchestrator.seats import _OPERATOR_ACTORS
    # Reviewed and left alone: this is a project-name sentinel filter ("operator" /
    # "analyst:operator" / "console" are never real repo names), not a live authorization
    # gate; no charter concept applies to whether a string is a genuine repository name.
    if not _REPO_NAME_RE.fullmatch(project) or project in _OPERATOR_ACTORS:
        return None
    matches = await actions.pool.fetch(
        "SELECT canonical FROM objects WHERE type='SoftwareProject' AND status='active' "
        "AND lower(canonical) = lower($1)", f"repo:{project}")
    canonical: str | None = matches[0]["canonical"] if len(matches) == 1 else None
    minted = False
    if canonical is None and not matches:
        name_matches = await actions.pool.fetch(
            "SELECT o.canonical FROM objects o WHERE o.type='SoftwareProject' "
            "AND o.status='active' AND lower((SELECT a.value #>> '{}' "
            "FROM current_assertions a WHERE a.object_id=o.id AND a.name='name' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)) = lower($1)",
            project)
        if len(name_matches) == 1:
            canonical = name_matches[0]["canonical"]
    if canonical is None and not matches:
        # A retired canonical is an alias, never a stub (a rename migrates the
        # canonical): a pin still spelling the pre-rename label resolves through
        # object_aliases to the migrated object's current canonical, so it neither mints
        # nor re-stamps `name` with the stale spelling.
        canonical = await actions.pool.fetchval(
            "SELECT o.canonical FROM object_aliases al JOIN objects o ON o.id=al.object_id "
            "WHERE al.type='SoftwareProject' AND al.alias=$1 AND o.status='active'",
            f"repo:{project}")
    if canonical is None:
        canonical = f"repo:{project}"
        # The corpse check, not the active-only one (guarded above by
        # test_register_agent_mount_never_resurrects_a_merged_husks_name): the two lookups
        # above filter to status='active' on purpose (a merged/retired object must never
        # win the case/name-collision arbitration), but that means a merged husk sharing
        # this exact canonical falls through here too - `create_or_find_object` below still
        # finds it (its find-or-create is on bare canonical, no status filter), it never
        # mints a new row. Asking "does any object already hold this canonical" (not just
        # an active one) is the only question that actually predicts whether the create
        # below mints; answering it with the active-only `matches`/`name_matches` result
        # would stamp `name` on the husk's own corpse and resurrect its pre-fold label.
        minted = not await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE type='SoftwareProject' AND canonical=$1", canonical)
    proj_id = await actions.create_or_find_object("SoftwareProject", canonical, actor)
    if minted:
        # Never mint without a name assertion (the general form of the operator-sentinel
        # fix above): unlike `_mint_or_find_repo` (capture.py), which has always stamped
        # `name` on a fresh mint, this path minted bare - every SoftwareProject reaching
        # /projects through mount()/mint_heir with no other name property fell straight to
        # resolve_label's canonical tier, leaking `repo:<label>` into the UI even for a
        # perfectly legitimate label. `project` here is the label that resolved the
        # canonical, so it is exactly what `name` should say.
        await actions.assert_property(proj_id, "name", project, actor,
                                      datetime.now(UTC), _CONF, evidence_class=_EC)
    return proj_id


async def mint_heir(
    actions: Actions, ancestor_id: str, ancestor_oid: uuid.UUID, *,
    because: str, succession: str | None, now: datetime | None = None,
    minting_door: str | None = None, upcoming_project: str | None = None,
    bind_seat: bool = True,
) -> tuple[str, uuid.UUID]:
    """Mint the next generation of a lineage: a new mind gets a new numeral, and the
    seams that count as a new mind include mid-session ones (live model swap,
    compaction), not just session death. Stamps the succession chain on both sides,
    passes the seat (handle) down, and re-addresses the ancestor's unread messages to the
    heir; the mailbox is part of the estate (a message sent to the old mind must reach
    whoever now holds the seat, or every compaction would orphan in-flight mail).

    Takes `ancestor_id`/`ancestor_oid` as given: folding any zero-turn phantom off the
    front of the chain is the caller's job, done via _fold_zero_turn_ancestors before
    this is called (both real callers do). Kept out of here on purpose: this function's
    return tuple is unpacked by roughly 20 call sites across the test suite, and
    threading the resolved ancestor back out would mean changing every one of them for a
    fact the caller already has in hand before it calls in.

    `upcoming_project`, when the caller already knows it (register_agent's own
    `identity.project`, moments before it asserts it; the heartbeat/live-swap call site
    has no such reading and leaves this None): this closes a duplicate-edge race. The
    house-relink below and register_agent's later identity.project assertion used to
    fire unconditionally in the same call, sharing one `now` - any divergence between
    the seat's derived `house` (often stale) and the session's fresh, correctly-resolved
    `identity.project` produced two live works_in edges on the heir, byte-identical to
    the microsecond, which move_agent_project_links then faithfully carries forward on
    every subsequent mint, forever, even after the underlying disagreement is corrected.
    This is the minimal write-side narrowing, not the full answer: it stops new
    duplicates; it does not retroactively heal the already-live specimens
    (invalidate_works_in is the per-lineage repair for those).

    `bind_seat` (default True): whether the seat's own `holds` link follows this mint
    (`follow_binding`, below). This fixes a holds-sandwich bug: `_bind_before_spawn`
    mints a fresh generation server-side before the session exists, purely so a
    pre-registered `agent_mounts` row exists for the spawned session's own automount()
    to re-attach through - that bookkeeping mint has done no real work and may never do
    any (it can be superseded again within seconds by the actual resume/launch outcome).
    Moving `holds` onto it anyway opened a real generation's own `holds` window into two
    pieces with a phantom sandwiched in between - the holds link is the fact of record
    for "who held this seat at time T," so this was wrong data regardless of how few
    callers ever asked that exact question. `_bind_before_spawn` passes
    `bind_seat=False` and re-binds the seat to its own resolved ancestor directly instead
    (never to the fresh bookkeeping heir); every other caller keeps the default,
    unchanged."""
    now = now or datetime.now(UTC)
    heir = next_generation(ancestor_id)
    # A mint never lands on a grave: after a same-lineage fold, the next numeral may name
    # a merged object - create_or_find would resurrect it and the estate transfer would
    # drag the living head's unread mail onto a corpse (witnessed: 10 unread on a merged
    # generation within the hour of its folding).
    #
    # A grave is a heal, not only a merge (live case: one lineage's generation xv): a
    # heal (husk-heal / phantom-fold) never flips objects.status away from 'active',
    # compensating events only, so a healed canonical passes the status check above while
    # still being a death in every sense that matters. Refuse to reuse it (refuse, don't
    # widen/search) rather than silently minting a real generation onto marks that record
    # a false start.
    #
    # But not every heal is a death; some are this same breath (caught by
    # test_two_zero_turn_compactions_fold before this shipped): _fold_zero_turn_ancestors
    # heals a zero-turn phantom and returns the corrected ancestor for this same
    # mint_heir call to mint against - next_generation() naturally reproduces the exact
    # numeral it just folded, and reusing it there is the fold's whole point (mint once,
    # not mint zero), not a resurrection. The two heal cases share the identical
    # false_mint/retired shape and are distinguished only by age: a heal still inside the
    # mint gate's own debounce window (_SEAM_DEBOUNCE_SECS, the same window the fold uses
    # for its own back-to-back check, not a second one) is part of the seam being
    # resolved right now; a heal older than that (20 hours cold, in one specimen) is a
    # closed one-way route.
    #
    # A plain, never-healed active object is ambiguous on status alone, and status alone
    # used to decide it (a specimen of stale-numeral reuse:
    # `test_mint_heir_never_duplicates_an_edge_the_heir_already_has` is the other,
    # legitimate half of this same shape, and any fix has to keep both true at once). Two
    # real cases share "exists, active, never healed": a bare stub some earlier call
    # already pre-seeded at this exact numeral for this same ancestor, waiting for this
    # mint_heir call to complete it (safe, intended to be adopted), and a real,
    # already-completed generation from a branch of this lineage's own history
    # (legitimately minted hours later by the real forward chain, and its own tip at the
    # time, equally real and equally not this call's to adopt). The two are told apart by
    # whether the candidate itself already carries a `succeeded_from` assertion, the one
    # thing a genuine mint always stamps on its own heir and a bare pre-seeded stub never
    # has, regardless of whether the stub's own successor was ever minted. A candidate
    # that is already a real generation is never a safe mint target, tip or not; the walk
    # steps past it, same as any other closed path.
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
            if healed_at is not None and (now - healed_at).total_seconds() <= _SEAM_DEBOUNCE_SECS:
                break
            if healed_at is None:
                already_real = await actions.pool.fetchval(
                    "SELECT 1 FROM current_assertions s WHERE s.object_id=$1 "
                    "AND s.name='succeeded_from' LIMIT 1", row["id"])
                if not already_real:
                    break
        heir = next_generation(heir)
    a = await actions.create_or_find_object("Agent", heir, heir)
    do = EvidenceClass.DIRECT_OBSERVATION
    await actions.assert_property(a, "succeeded_from", ancestor_id, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    await actions.assert_property(a, "minted_because", because, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    # The parallel-lives stamp: mount rows are hot state, so the pulse evidence at mint
    # time must be captured at the mint or it is gone by lint time. Stamp the
    # predecessor lineage's freshest pulse; and when a different session than the one
    # minting held a live pulse (view rows excluded, the alias is never the witness),
    # stamp that session too. graph_lint reads the stamps and alarms; the mint itself
    # always proceeds (report-only downstream).
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
    # The house passes with the lineage: heartbeat-minted heirs got a project assertion
    # later but never the works_in edge, so every lens that walks the edge missed them.
    # Inherit both here, once, for every mint path; the register path re-stamps its own
    # reading afterwards and the byte-dup skip absorbs the overlap.
    #
    # Two different questions share this neighborhood, with deliberately separate
    # variables: `house` below stays exactly the seat-derived-house-or-raw-stamp
    # computation this function always used. Generation counting (`seat_holders`, further
    # down) compares each historical holder's own raw project stamp against it, the same
    # discipline claim_name's own `counting_house` keeps (further above) for the
    # identical reason: counting must compare raw stamps to raw stamps, never a resolved
    # display value, or a seat whose true history all shares one raw stamp starts
    # undercounting the moment this function's other concern (below) stops treating that
    # stamp as authoritative.
    #
    # `heir_project` is the new, separate answer for what the heir's own project stamp
    # should be: copying `ancestor_seat["house"]`/`house_of(ancestor_id)` here (the old
    # single unified `house` this comment used to describe) was itself the class of
    # fabrication being closed - Seat.house is a mint-time stamp (`=handle` for every
    # self-managed seat), not the ancestor's real project, and a single polluted copy
    # propagated forward forever through every automatic mint (every compaction, every
    # model swap, every session death). `project_of` (no `cwd`, this call site has never
    # read a pin and isn't gaining a new filesystem read here) resolves it instead:
    # charter (if the ancestor's seat declared exactly one repo) then lineage works_in
    # (merge-normalized), house nowhere in it, homeless a legal answer. The `and not
    # moved and not upcoming_project and not chartered` gate below, and everything
    # move_agent_project_links/chartered compute, is unchanged; only the project-stamping
    # fallback's source moved.
    from src.orchestrator.seats import held_seat
    ancestor_seat = await held_seat(actions.pool, ancestor_id)
    house = (ancestor_seat["house"] if ancestor_seat and ancestor_seat.get("house")
            else await house_of(actions.pool, ancestor_id))
    heir_project = await project_of(actions.pool, ancestor_id)
    # The fallback retires once a charter exists: works_in means exactly one thing now,
    # the session's live/current project; the seat's durable role-house lives on
    # `governs` (re-keyed onto the Seat itself), not on this edge. `governs` is not
    # written here to replace it - set_charter declares the whole charter each call
    # ("these are the repos this seat rules now, not an increment"), so auto-firing it
    # from every mint with just `house` would silently heal away the rest of a real
    # multi-repo charter (one seat's charter spans six repos, charter.py's own example)
    # the moment its lineage next compacted. charter_of is read-only and additive-safe:
    # once a seat has declared any charter, this fallback has nothing left to do (governs
    # already durably answers "which house"), so it stops firing for that seat; a seat
    # that has never declared one keeps today's behavior unchanged (charter_of's own
    # docs: "works_in still names its home" until it does).
    from src.orchestrator.charter import charter_of
    chartered = (bool(await charter_of(actions.pool, ancestor_seat["seat_id"]))
                if ancestor_seat else False)
    # The mint_heir edge leak, closed (measured 906 of 6,245 fleet-wide): mint_heir
    # minted a fresh works_in edge for the heir below but never touched the ancestor's
    # own, so every past generation of a lineage that ever asserted works_in/governs kept
    # it live forever, through ordinary succession, growing on the most common event in
    # the fleet. Move whatever the ancestor still has live (which may be more than just
    # `house`, an agent can work_in/govern several projects across its life) onto the
    # heir, invalidate+create, before stamping the heir's own current house below
    # (idempotent either order; move_agent_project_links never duplicates a link already
    # live on the heir).
    moved = await move_agent_project_links(actions, ancestor_oid, a, heir, now)
    # The race, narrowed: this used to _link_once `house` unconditionally, regardless of
    # what move_agent_project_links just carried forward or what register_agent (this
    # call site's own caller) is about to assert moments later in the same call, sharing
    # this same `now` - the shared timestamp is why the duplicate lands byte-identical
    # rather than merely close. `house` is only ever the sole source of truth for the
    # heir's project when nothing else is: skip it the moment either
    # move_agent_project_links found something live to carry forward, or the caller
    # already knows a fresher project is coming right behind it, or the seat has since
    # declared a charter, so `house` is stale legacy inference and governs is the fact of
    # record instead.
    if heir_project and not moved and not upcoming_project and not chartered:
        await actions.assert_property(a, "project", heir_project, heir, now, _CONF,
                                      evidence_class=_EC)
        proj = await _resolve_or_mint_project(actions, heir_project, heir)
        if proj is not None:
            await _link_once(actions, a, proj, "works_in", heir, now)
    # Seat inheritance (phase 2): the heir inherits the ancestor's human name, the seat
    # passes down the lineage, the generation (roman numeral) ticks up. "Anna" becomes
    # "Anna II".
    inherited = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='handle' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1",
        ancestor_oid)
    if inherited:
        await actions.assert_property(a, "handle", inherited, heir, now, _CONF,
                                      evidence_class=_EC)
        # ...and the seat passes with the name, or the name is just a label. (A specimen
        # of the earlier gap: one heir was minted carrying its predecessor's handle with
        # no generation edge to the mind whose work it continued.) This is where a seat
        # changes hands without a handoff: mint_heir is the automatic succession, it
        # fires on every compaction, every model swap, every session death, and it passed
        # the name down while leaving the seat's chain broken. Only claim_name(), an
        # explicit act by a mind that thinks to call it, ever minted the edge. A
        # historical backfill closed 77 existing gaps but never fixed the code that omits
        # them, so the chain healed and then broke again at the very next heir minted
        # after the heal. Left alone it would re-open the gap at every compaction,
        # forever.
        #
        # succeeds_seat is not succeeded_from (stamped above): that one chains anchors
        # (which conversation spawned which) and this one chains holders of a job. Two
        # relations wearing one name is the mistake that started all of this. (`house`
        # resolved above, where the heir inherited it.)
        holders = [h for h in await seat_holders(actions.pool, house, inherited) if h != heir]
        await actions.assert_property(a, "seat_generation", str(len(holders) + 1), heir, now,
                                      _CONF, evidence_class=_EC)
        await _link_once(actions, a, ancestor_oid, "succeeds_seat", heir, now)
    # The binding follows the head: every Seat object the ancestor actively holds
    # re-links to the heir - the durable address must keep pointing at whoever the mind
    # is now, or the first compaction after an attach would strand the seat on a corpse.
    # The old link heals by valid_until; holder history stays walkable. Gated on
    # bind_seat (see the docstring's own holds-sandwich paragraph): a caller that knows
    # this heir is pure pre-spawn bookkeeping, never yet a real occupant, passes
    # bind_seat=False so the seat's own holds history never opens a window for it at all.
    if bind_seat:
        from src.orchestrator.seats import follow_binding
        await follow_binding(actions, ancestor_oid=ancestor_oid, heir=heir, heir_oid=a, now=now)
        # The hole stops regenerating: follow_binding above only moves a holds link the
        # lineage already carries - a seat whose original claim predates the Seat-object
        # binding never got one in the first place, and nothing automatic ever calls
        # claim_name for it. The backfill cures every such seat that exists today; left
        # here, the very next mint of that same lineage would re-open the identical hole,
        # forever, because mint_heir fires on every compaction/model-swap/session-death
        # and nobody asks it to. So: if the handle just inherited names an existing Seat
        # object with no active holder anywhere, bind it now, the same self-heal
        # claim_name performs explicitly, run at the one moment that requires no one to
        # think to call it. Never mints a new Seat (ensure_seat's own rule: minting is
        # deliberate, only at a claim or an attach); this only closes a hole that already
        # has a name.
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
    # ...and so does the read state: the heir inherits the ancestor's recipient rows, or
    # every mint (i.e. every compaction) would redeliver the project's whole settled
    # broadcast history to the new mind. The heir literally remembers reading them, that
    # memory is exactly what survived the seam.
    #
    # But only the settled half: a message_recipients row with read_at IS NULL is a
    # lease, not a memory - the ancestor read it (delivered_at) but never replied or
    # acked before dying, and a lease belongs to the mind that holds it, not to whoever
    # inherits its name next. The old unconditional copy carried the ancestor's own
    # (recent) delivered_at straight onto the heir, so the heir's very next inbox() read
    # the message as "already delivered inside its lease window" and stayed silent about
    # it for as long as that lease had left to run - a genuinely unread ask going dark
    # across the one seam that most needs it surfaced (one specimen: two leased asks read
    # as settled by the mint, neither ever answered). Filtering to read_at IS NOT NULL
    # here is the fix: a truly settled message still carries its memory forward exactly
    # as before; a merely-leased one carries nothing, so the heir's own next inbox()
    # finds it with no prior row at all - fresh, undelivered, exactly as if this mind
    # were reading it for the first time, which it is.
    await actions.pool.execute(
        "INSERT INTO message_recipients (message_id, agent_id, delivered_at, read_at, deliveries)"
        " SELECT message_id, $1, delivered_at, read_at, deliveries FROM message_recipients"
        " WHERE agent_id=$2 AND read_at IS NOT NULL"
        " ON CONFLICT (message_id, agent_id) DO NOTHING", heir, ancestor_id)
    return heir, a


async def fold_existing_zero_turn_phantoms(actions: Actions) -> list[dict[str, Any]]:
    """RETROACTIVE CLEANUP: the going-forward fix (mint sites call
    _fold_zero_turn_ancestors before minting) does nothing for generations already
    minted before this fix landed, like the canonical repro itself (minted by /compact,
    superseded by /model before its first turn). Sweeps every already-superseded,
    already-minted Agent (has succeeded_from and succeeded_by, so a live descendant
    exists) that isn't already false_mint, folding each one exactly the live path would
    have. Safe to run repeatedly: an already-folded phantom carries false_mint and is
    excluded by construction; a half-folded one (flagged but never unwound) is reported,
    not re-attempted, and open_thread's own idempotency keeps repeat sightings from
    paging more than once. Returns what it folded, for the record; a half-healed
    sighting is not counted here, only in the obligation it opens."""
    # A generous pre-filter, deliberately: every minted (non-root) Agent, live head
    # included. Correctness rests on _fold_zero_turn_ancestors's own agent_has_acted
    # gate, not on this query, so a live head with real acts (or an already-folded
    # phantom, whose walk halts at itself just as harmlessly) is a fast, safe no-op
    # rather than something this query must itself get exactly right (the
    # value-comparison this would otherwise need, "is succeeded_by currently
    # non-empty", is exactly the winning-row read the SQL hygiene tripwire exists to
    # keep out of a bare EXISTS).
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


# Notify at seam: a compacting worker session's manager should learn from the fleet, not
# from the human noticing. Only a harness-reported context death fires this, the silent
# class nobody else is watching. Model-succession and live-swap already surface on the
# existing danger map; reanimation-of-retired is a deliberate act, not an accident that
# strands a manager mid-conversation. Known v1 gap: reanimation co-occurring with a real
# compaction is excluded too - when both fire, mint_because reads "reanimation-of-retired",
# never "compaction", so it never matches this whitelist. Left this way on purpose: it's
# rare, and widening the whitelist now would trade v1's whole value (precision on the
# silent class) for a case nobody's been bitten by yet. A successor who is bitten by it
# finds the gap named here, not rediscovered.
_SEAM_NOTIFY_REASONS = {"compaction", "context-clear"}


async def _notify_seam_manager(
    actions: Actions, *, heir: str, mint_because: str, project: str | None,
) -> None:
    """A worker that just silently died and came back messages its own manager. A clean
    repro of the underlying problem: a mail send-result refused the manager's message to
    a fresh successor while the daemon held a live job the whole time, and the human had
    to notice and flag it. This is the fix: the successor reports itself, with the
    daemon's own reachability() evidence inline, so the manager gets a confirmation, not
    just a claim. Silent no-op when there's no seat or no manager of record, the same
    "nobody to confess to" shape the stop-hook confession already uses."""
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
    """A mind is witnessed by its acts: did this agent ever do anything beyond its own
    mint/registration bookkeeping? Acts = assertions on objects other than the excluded
    lineage pair, words sent, or mail settled after the mint. Not acts: the display-name
    stamps registration writes onto the repo and principal objects (routine registration
    paperwork - the register path stamps those on every mount, and counting them made
    every register-minted heir read as a mind, so the cross-path debounce could never
    heal one)."""
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
    """Debounce for the generation-succession seam: toggling the model setting back and forth
    within a short window used to mint a new generation for each toggle, diluting what a
    generation change is supposed to mean (a new generation tracks a new mind, not a settings
    change). The distinction that keeps both concepts intact: a mind is witnessed by its acts.
    When the model returns to the left side of the seam within the window and the transient
    heir asserted nothing beyond its own mint bookkeeping, sent nothing, and settled nothing,
    no independent mind ever existed; the mint heals as false (event-sourced, compensating,
    its record stays) and the ancestor takes its seat back, including its prior state. One
    witnessed act, and the heir stands: a real mind passed through, however briefly. Returns
    the heal dict, or None if the mint stands.

    Shared by both mint paths: this originally lived only in the heartbeat check and only
    healed heads minted via 'live-swap', so a round-trip whose return leg was witnessed by a
    mount (register_agent) could never heal, and the two observers could ping-pong generations
    off each other's stamps. `agent_id` must be the lineage head; `job_dir` re-points that
    mount row when the caller has one, else any row naming the healed heir follows the
    restored ancestor."""
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
    # Both model-seam mints heal; a compaction/clear/reanimation mint is a context death,
    # not model flapping, so there is no "left side" to return to.
    if meta.get("minted_because", (None, None))[0] not in ("live-swap", "model-succession"):
        return None
    ancestor, minted_at = meta.get("succeeded_from", (None, None))
    seam = meta.get("model_succession", ("", None))[0] or ""
    if (not ancestor or minted_at is None
            or (now - minted_at).total_seconds() > _SEAM_DEBOUNCE_SECS):
        return None
    left = normalize_model(seam.split("→")[0].strip()) if "→" in seam else None
    if left is None or left != observed:
        return None  # not a round-trip: a third model is a real third mind
    ancestor_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent'", ancestor)
    if ancestor_oid is None:
        return None
    # Acts = assertions beyond the lineage bookkeeping pair, messages sent, or mail settled
    # after the mint (a lease/delivery is passive perception, never an act).
    if await agent_has_acted(actions, cur, exclude=[cur_oid, ancestor_oid],
                             settled_after=minted_at):
        return None
    do = EvidenceClass.DIRECT_OBSERVATION
    conf = confidence_for(do)
    # Atomic: these six writes either all land or none do. Previously they were six
    # independent unguarded statements, so a crash or interruption partway through left the
    # flag stamped but the pointer never unwound, a half-healed state permanently invisible
    # to any reader that treats the flag alone as "done" (the exact shape found in several
    # live cases, weeks later, with real generations already minted on top; completing that
    # heal retroactively would have rebound the current live seat backward onto a stale
    # ancestor). One transaction closes the gap: after this, false_mint == "true" is proof
    # of completion again, not merely of an attempt.
    from src.orchestrator.seats import follow_binding
    async with actions.atomic() as a:
        for k, v in (("false_mint", True), ("retired", True), ("retired_by", _DEBOUNCE_SRC),
                     ("false_mint_because",
                      "model round-trip within the debounce window, no witnessed act — "
                      "settings churn, not a death (Soundwave's grievance, b813e389)")):
            await a.assert_property(cur_oid, k, v, _DEBOUNCE_SRC, now, conf,
                                    evidence_class=do.value)
        # Unwind the head-walk (the old pointer stays in history: compensating, never deleted).
        await a.assert_property(ancestor_oid, "succeeded_by", "", _DEBOUNCE_SRC, now, conf,
                                evidence_class=do.value)
        # State returns to the restored mind: unread messages re-address to it; read state
        # needs no unwind (the heir's copied rows are inert once the heir is retired).
        await a.execute(
            "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
            ancestor, cur)
        # ...and so does the seat binding: mint_heir moved the holds link to the transient
        # heir; a heal that leaves it there strands every seat-addressed message on a false
        # mint the read predicate still honors. The binding follows the head, and after a
        # heal, the head is the restored ancestor.
        await follow_binding(a, ancestor_oid=cur_oid, heir=ancestor,
                             heir_oid=ancestor_oid, now=now)
        if job_dir is not None:  # the heartbeat's caller holds the row, bump its pulse too
            # This heal is itself a repair of drifted bookkeeping, not the earning act: it
            # may only refresh a pulse the row already earned, never grant one.
            await a.execute(
                "UPDATE agent_mounts SET agent_id=$2, model=$3, "
                "last_seen=CASE WHEN earned_pulse_at IS NOT NULL THEN now() ELSE last_seen END "
                "WHERE job_dir=$1", job_dir, ancestor, observed)
        else:  # the register path: any row naming the healed heir follows the restored mind
            await a.execute(
                "UPDATE agent_mounts SET agent_id=$2, model=$3 WHERE agent_id=$1",
                cur, ancestor, observed)
    return {"healed": cur, "restored": ancestor,
            "seam": f"{seam} → {observed} (round-trip within "
                    f"{_SEAM_DEBOUNCE_SECS // 60}m, no act — debounced, not a death)"}


async def _already_reached(actions: Actions, *, agent_id: str, observed: str) -> bool:
    """Did this lineage head already record a swap landing on `observed`? This guards against
    a real live repro where a single real model transition produced three separate
    'live-swap' mints because of idempotency gaps in the succession check. The comparison
    live_succession runs the seam against, agent_mounts.model, is a mutable row that can
    drift back to a stale value after the real swap already completed (mount()'s own
    re-derivation resets it; the deeper cause is tracked separately). The head's own
    `source_model` is equally mutable, reset by the same path. `model_succession` is not:
    mint_heir stamps it exactly once, at the mint that recorded the swap, and nothing ever
    touches it again, making it the one write-once witness immune to the drift. If the
    head's own recorded transition already landed on `observed`, a fresh "stored != observed"
    reading is re-discovering a completed swap, not witnessing a new one; minting again would
    just create a duplicate generation for one transition. A genuinely new target (observed
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
    """A mid-session model change, sensed by the heartbeat check: the mind changed under a
    live session, so the seat passes now. Mint the heir, move the durable mount row, and
    every per-render read (statusline, stop hook, digest) resolves to the new mind from the
    next glance. Idempotent: an unchanged model or an unknown mount is a no-op; a row with no
    stored model gets a first stamp, not a succession (you can only succeed something that
    existed)."""
    sid = (session_id or "").strip().lower()
    observed = normalize_model(observed_model)
    if len(sid) < 8 or not observed:
        return {"unchanged": True, "reason": "no anchor"}
    # The one session-to-row lookup (mounts.find_session_row). An inline copy here once
    # meant a swap in a re-anchored window went unwitnessed.
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
        # Re-read inside the lock: two concurrent heartbeats once both read the pre-swap row
        # and both minted, producing identical seam strings a second apart. The loser now
        # waits, re-reads, sees the winner's write, and no-ops.
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
        # The null-seam gate, mirroring the same check in forks.py as defense-in-depth: a row
        # with no stored model was never observed, not observed-as-something-else, so it can
        # never "disagree with" the first real reading. This is a first stamp, never a seam
        # to mint against.
        if old is None:
            await actions.pool.execute(
                "UPDATE agent_mounts SET model=$2 WHERE job_dir=$1", row["job_dir"], observed)
            return {"unchanged": True, "reason": "first stamp"}
        now = datetime.now(UTC)
        # Seams run on the lineage head, and the debounce judges the head, not the row,
        # which may lag its own succession.
        head = await lineage_head(actions.pool, row["agent_id"])
        # A there-and-back model toggle with no act between heals instead of minting again.
        healed = await _debounce_roundtrip(actions, agent_id=head, observed=observed,
                                           now=now, job_dir=row["job_dir"])
        if healed is not None:
            return healed
        # Idempotency: the head already recorded reaching this exact model once. A fresh
        # disagreement against the (mutable, driftable) stored row is the same completed
        # swap resurfacing, not a new one. Repair the drifted stamps in place; mint nothing.
        if await _already_reached(actions, agent_id=head, observed=observed):
            do = EvidenceClass.DIRECT_OBSERVATION
            head_oid = await actions.create_or_find_object("Agent", head, head)
            await actions.assert_property(head_oid, "source_model", observed, head, now,
                                          confidence_for(do), evidence_class=do.value)
            # Repairing a drifted model stamp is not the earning act: refresh the pulse only
            # if this row already earned one.
            await actions.pool.execute(
                "UPDATE agent_mounts SET model=$2, "
                "last_seen=CASE WHEN earned_pulse_at IS NOT NULL THEN now() ELSE last_seen END "
                "WHERE job_dir=$1",
                row["job_dir"], observed)
            return {"unchanged": True,
                   "reason": f"idempotent — {head} already recorded reaching {observed}; "
                             "repaired the drifted stored model, minted nothing"}
        # Whose hand moved the model? A model change on this session's own transcript makes
        # the seam the operator's deliberate act. The mint still happens (a mind change is a
        # mind change regardless of cause) but the seam string carries who caused it, so no
        # downstream surface misreports it.
        deliberate = False
        try:
            main = locate_current_transcript(
                Path.home() / ".claude/projects", row["job_dir"], anchored_only=True)
            if main is not None:
                _cur, _hist, deliberate = await model_of_transcript(main)
        except OSError:
            deliberate = False
        # The earned-pulse gate: this is the one path that mints a new agent generation off
        # the statusline's own observed model alone. "The heartbeat's model is as anchored as
        # a transcript read" (see below) conflates a truthful render with an earned act: a
        # spare session that never took a turn can render a statusline forever without ever
        # earning a pulse. Refuse to mint off a row that has never proven itself alive; a
        # measured incident this closes involved a generation minted but never acted upon,
        # with exactly this shape.
        earned = await actions.pool.fetchval(
            "SELECT earned_pulse_at FROM agent_mounts WHERE job_dir=$1", row["job_dir"])
        if earned is None:
            return {"unchanged": True,
                   "reason": "no earned pulse on this row — refusing to mint an heir off "
                             "an unearned observation (THE EARNED-PULSE COLUMN, mail 9873)"}
        ancestor_oid = await actions.create_or_find_object("Agent", head, head)
        # Succession follows turns: fold any zero-turn phantom off the front of the chain
        # before minting on top of it. head/ancestor_oid below name whoever this heir
        # actually succeeds, not a compaction-minted phantom that never took a turn.
        head, ancestor_oid = await _fold_zero_turn_ancestors(actions, head, ancestor_oid, now)
        seam = f"{old} → {observed}" + (" [operator /model]" if deliberate else "")
        heir, heir_oid = await mint_heir(actions, head, ancestor_oid, because="live-swap",
                                         succession=seam, now=now,
                                         minting_door=row["job_dir"])
        # The heartbeat's model is the harness's own word about a session it is rendering, as
        # anchored as a job_dir transcript read, and the baseline the next seam check runs
        # against (without it, a later re-mount would see no anchored model on the heir and
        # stay quiet).
        do = EvidenceClass.DIRECT_OBSERVATION
        await actions.assert_property(heir_oid, "source_model", observed, heir, now,
                                      confidence_for(do), evidence_class=do.value)
        if row["project"]:
            await actions.assert_property(heir_oid, "project", row["project"], heir, now, _CONF,
                                          evidence_class=_EC)
        # A prior version of this code tried `_job_id(job_dir)` first here, backwards from
        # the precedent this same module already sets elsewhere (`session or
        # _job_id(job_dir)`: explicit session wins, job_dir is only ever the fallback
        # guess). For a resume-style wake, job_dir is per-session, so the two values happen
        # to agree and the bug never showed. For a background-launched seat, job_dir is the
        # durable per-seat anchor (`_launch_anchor`'s own `jobs/seat-<hex>`, unchanged
        # across every generation), and `_job_id` has no way to know that isn't a session
        # id, so it dutifully returns the seat's own canonical, stamped as if it were one.
        # This was confirmed live in an agent whose graph record carried a seat-anchor slug
        # as its session id while its real session id sat, findable, right where its
        # mount's transcript actually was: the corrupted stamp was the whole reason resume
        # reported "mounted but no transcript found on disk". `sid` is already validated
        # non-empty (the `len(sid) < 8` guard above returned early otherwise), so it is
        # never a worse choice than a job_dir guess and goes first now, matching this
        # module's own established precedent. The `_job_id` fallback below is effectively
        # unreachable in practice (`sid[:8]` on an already-length-checked `sid` is never
        # falsy), guarded anyway for the same reason register_agent's own birth-time write
        # is: never let a stable-anchor slug wear the `session` column
        # (`_looks_like_a_real_session`).
        sid_prop = sid[:8] or _job_id(row["job_dir"])
        if _looks_like_a_real_session(sid_prop):
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
    """True if this Agent carries a winning retired=true, meaning a deliberate close. Read off
    the projected current_assertions (highest confidence, then most recent), the same
    predicate the trigger's reanimation guard uses, so mount and wake agree on whether this
    identity is closed."""
    v = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions "
        "WHERE object_id=$1 AND name='retired' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", agent)
    return bool(v == "true")


# A real harness session id, truncated, is never the shape `_job_id` hands back for a
# stable anchor: every genuine sid this codebase ever produces (a heartbeat's own
# `session_id`, a transcript filename's leading segment, a UUID slice from an external
# source) is exactly 8 lowercase hex characters (`_UUID_RE` in ingest/sessions.py validates
# the same shape at 36 chars before this module's own `[:8]`/`.split("-")[0]` truncations
# run). `_launch_anchor`'s own `jobs/seat-<hex>` is not that shape (a literal seat
# canonical, unrelated to any session), so `_job_id` returns it unvalidated; its "jobs"
# branch, unlike its own "sessions" branch twenty lines below it, never checks. The fix
# deliberately lives here, not in `_job_id` itself: `_job_id`'s return value does two jobs,
# a session id and (via `sid`/`identity.agent_id=f"agent:{sid}"` in resolve_identity) a
# durable identity anchor for a session with no other observation at all. Pure-seat-office
# lineages bootstrap their very first identity through exactly that anchor fallback, so
# narrowing `_job_id` itself would silently rename every future first-ever mount of that
# shape from a readable `agent:seat-<hex>` to an opaque `agent:j<hash>` (resolve_identity's
# own "last resort" branch), the kind of regression a prior fix in this codebase warned
# against: a shared value doing two jobs, where the obvious fix to one breaks the other.
# Gating only the `session` property write, never `_job_id`/`sid`/`agent_id` themselves,
# fixes the resume-poisoning defect with zero anchor-role risk.
_SESSION_ID_RE = re.compile(r"^[0-9a-f]{8}$")


def _looks_like_a_real_session(sid: str | None) -> bool:
    """Is `sid` shaped like a genuine (truncated) harness session id, never a stable
    per-seat anchor slug or any other non-session string wearing its column? See the module
    comment just above for the full reasoning."""
    return sid is not None and _SESSION_ID_RE.match(sid) is not None


async def register_agent(
    actions: Actions, identity: AgentIdentity, *, actor: str, expected_model: str | None = None,
    mint_reason: str | None = None, revisit_check: bool = False,
) -> uuid.UUID:
    """Mint (idempotently) the Agent object and its org-chart links. The agent attributes its
    own registration (`source = agent:<session>`), self-declared. Re-mount is a no-op
    (find-or-create plus the kernel's byte-dup assertion skip absorb it). `expected_model`
    (the operator's standing choice) turns on the swap detector: the intent is stamped, and a
    silent demotion away from it is recorded as a first-class observed event on the Agent.

    The succession seam: session-keyed identity means a retire-compact-swap sequence hands a
    dead agent's id to a fresh context, a different model then writes as it, and the
    transcript-level swap detector is blind when the new transcript never ran the old model.
    So registration also compares the fresh anchored observation against the graph's last
    anchored source_model: any disagreement is a succession seam under the standing rule that
    a generation tracks a mind, even one the transcript witnessed. An older exemption for
    witnessed transitions ("same context, different seam") encoded tenure semantics that has
    since been overruled: the generation number tracks which mind, and a mind is one
    contiguous run of one model, so a witnessed swap is a succession like any other (the
    warm-swap `model_swapped` stamp still lands too; both records are true). `mint_reason`
    forces a mint for a context death the harness reported with no model change at all
    (compaction, session clear): the weights survive but the memory the operator was talking
    to does not.

    `revisit_check=True` is opt-in, load-bearing only at the one call site that genuinely
    needs it (_reattach's own transcript self-restore fallback in mcp_server.py, audited
    live: a real prior transcript proves the session ran before, but nothing links it to any
    known lineage, and it minted unconditionally with no check at all before this). When a
    genuinely fresh Agent object mints under this flag at a project already carrying activity
    from a different, unrelated lineage, `_flag_unattributed_revisit` opens a loud obligation
    thread naming it instead of minting silently; it never refuses the mint itself, only
    confesses it. False by default: every other register_agent call site already carries its
    own attribution (a fork's spawned_by link, an office-birth's deed, a seam heir's own
    parent generation) that this check has no way to see, and must never second-guess."""
    now = datetime.now(UTC)
    obs: str | None = None
    # The mint lock: phases 0-1 read-then-write the succession chain; two concurrent
    # registrations (or a registration racing the heartbeat) must serialize per lineage, or
    # the loser's head-walk finds the winner's mint and stacks a generation on it.
    async with mint_lock(actions.pool, _generation(identity.agent_id)[0]):
        # Phase 0, lineage: a session-keyed resolve lands on the base id; walk to the lineage
        # head first, since the head is who this name is now. Seam checks run against the head.
        head = await lineage_head(actions.pool, identity.agent_id)
        if head != identity.agent_id:
            identity.agent_id = head
        src = identity.agent_id
        # A plain pre-existence check, not a change to create_or_find_object's own shared
        # return contract: every other caller of that function expects a bare id back, and
        # widening it here would ripple across the whole codebase for one caller's own
        # question. Only computed under revisit_check (an extra read on the primary mint path is
        # a real cost, paid only by the one call site that asked for it).
        genuinely_fresh = revisit_check and not bool(await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE type='Agent' AND canonical=$1", identity.agent_id))
        a = await actions.create_or_find_object("Agent", identity.agent_id, src)

        # Phase 1, seam detection leading to mint: the heir gets its own name.
        mint_because: str | None = None
        if await _winning_retired(actions, a):
            # A retired identity is never re-worn: the arriving context is an heir and gets
            # minted below. The retirement stands.
            mint_because = "reanimation-of-retired"
        anchored = bool(identity.model) and identity.model_method == "job_dir"
        if anchored:
            # The succession seam: read the baseline before the new observation supersedes
            # it. There is no witnessed-transition exemption: oscillation mints every time,
            # since the returning model is a third mind, not the first one back. Normalized
            # comparison: a bracketed display variant of the same weights is the same mind,
            # never a seam.
            prior_raw, prior_at = await _last_anchored_stamp(actions, a)
            prior = normalize_model(prior_raw)
            obs = normalize_model(identity.model)
            # The null-seam gate, defense-in-depth for the same lesson applied in forks.py: a
            # null or "unknown" prior is the absence of an observation, never a value; "we
            # have not looked yet" is not "the mind was someone else". A fresh reading can't
            # disagree with a null, so `prior is not None` must gate every comparison below it.
            if prior is not None and prior != obs:
                # The dating gate: the transcript tail lags a model change, since no
                # assistant turn has run on the new model yet, so an observation not fresher
                # than the stamp it disagrees with is stale data arguing with a current
                # reading, never a real seam. (One prior incident showed opposite seams
                # minted off each other's stale reads, four seconds apart.) An unstamped
                # observation keeps the old behavior: the gate only ever suppresses a mint it
                # can prove stale.
                stale = (identity.model_observed_at is not None and prior_at is not None
                         and identity.model_observed_at <= prior_at)
                if not stale:
                    identity.model_succession = f"{prior} → {obs}"
                    mint_because = mint_because or "model-succession"
        if mint_reason:
            # A harness-reported context death (compaction, session clear) with no model
            # seam of its own.
            mint_because = mint_because or mint_reason
        if mint_because == "model-succession" and not mint_reason and obs is not None:
            # A model seam alone may be settings flapping: try the heal before minting. The
            # debounce must work whichever observer witnesses the return leg (it used to
            # live only in the heartbeat, so a mount seeing the round-trip minted a phantom
            # generation).
            healed = await _debounce_roundtrip(actions, agent_id=identity.agent_id,
                                               observed=obs, now=now)
            if healed is not None:
                identity.agent_id = str(healed["restored"])
                src = identity.agent_id
                a = await actions.create_or_find_object("Agent", identity.agent_id, src)
                identity.model_succession = None
                mint_because = None
        if mint_because == "reanimation-of-retired":
            # `identity.agent_id`/`a` here is the retired object itself. Handing it to
            # `_fold_zero_turn_ancestors` unchanged used to chain the new heir onto a folded
            # phantom whenever the retiree also happened to be false_mint (its own
            # first-iteration halt treats an already-false_mint starting node as "a prior
            # fold already resolved this", correct mid-walk, wrong as an entry point).
            # Skip forward past any false_mint ancestors first, landing on the nearest
            # eligible one before the fold walk ever runs.
            identity.agent_id, a = await _skip_false_mint_ancestors(actions, identity.agent_id, a)
        if mint_because:
            # Succession follows turns: fold any zero-turn phantom off the front of the
            # chain before minting. succeeded_from must land on whoever this heir actually
            # succeeds, not a phantom that never took a turn (the exact gap that once left
            # orient()'s inheritance block blind on a double-mint).
            identity.agent_id, a = await _fold_zero_turn_ancestors(
                actions, identity.agent_id, a, now)
            identity.succeeded_from = identity.agent_id
            heir, a = await mint_heir(actions, identity.agent_id, a, because=mint_because,
                                      succession=identity.model_succession, now=now,
                                      minting_door=identity.session,
                                      upcoming_project=identity.project)
            identity.agent_id = heir
            src = heir
    if identity.succeeded_from is None:
        # `succeeded_from` is written onto `identity` in exactly one place in this whole
        # module: the mint branch just above, for the single turn a seam is actually
        # detected. Every later call for the same (already-minted, already-head) agent
        # builds a brand-new `AgentIdentity` via `resolve_identity()` (no pool, no DB
        # access, `succeeded_from` stays its dataclass default of None), so before this
        # fix, any MCP server bounce or fresh connection after the one true mint turn
        # permanently blinded that session's own succession signal: orient()'s `if ident
        # and ident.succeeded_from:` gate never opened again for the rest of that agent's
        # life, even though the Agent's own `succeeded_from` property in the graph was
        # correct the entire time (confirmed live in a case at hop distance 1 to its real
        # predecessor, with the DB row correct but the session-side field simply never
        # repopulated: pure read-side loss, not a write bug). Recovered here via the same
        # primitive `nearest_handoff_ancestor`'s own walk already uses (`_succeeded_from_of`),
        # at the one place this function already touches the DB for this exact agent: cheap
        # (a single indexed point lookup, the same shape as
        # `_winning_retired`/`_last_anchored_stamp` just above, not a new query class on
        # this primary code path) rather than widening `resolve_identity` with a pool it was never
        # given. Guarded on `is None` so a genuine mint this same turn (which already set
        # the correct value above) is never overwritten by a redundant read.
        identity.succeeded_from = await _succeeded_from_of(actions.pool, identity.agent_id)
    if mint_because is not None and mint_because == mint_reason and (
        mint_reason in _SEAM_NOTIFY_REASONS
    ):
        try:
            await _notify_seam_manager(actions, heir=src, mint_because=mint_because,
                                       project=identity.project)
        except Exception as exc:  # noqa: BLE001 : a prior bug here was silent failure; a
                                   # notify failure must never be the thing that blocks a
                                   # mount, but swallowing it without a trace would just
                                   # relocate the same silence one layer down. Fail open,
                                   # never fail quiet.
            logger.warning("notify-at-seam failed for heir %s (%s): %r",
                           src, mint_because, exc)
    label = f"{identity.model or 'claude'} in {identity.project or '?'}"
    await actions.assert_property(a, "name", label, src, now, _CONF, evidence_class=_EC)
    # The birth-time write: the first-ever mount of a background-launched seat has nothing
    # else to observe yet and `identity.session` falls all the way to `_job_id(job_dir)`,
    # the seat's own stable anchor slug, not a session id (see `_looks_like_a_real_session`'s
    # own comment). Skipping this assert rather than writing the anchor as `session` leaves
    # the property correctly absent, never a confident lie, until a later call (the
    # heartbeat's own live_succession, or a fresh resolve_identity once this session's
    # transcript actually exists) has something real to stamp. The resume command's
    # resident-unknown gate already treats "no signed testimony" as an honest unknown, never
    # a refusal shaped like corruption.
    if _looks_like_a_real_session(identity.session):
        await actions.assert_property(a, "session", identity.session, src, now, _CONF,
                                      evidence_class=_EC)
    await actions.assert_property(a, "identity_resolved", identity.resolved, src, now, _CONF,
                                  evidence_class=_EC)
    if identity.model:
        ec = _MODEL_EC.get(identity.model_method or "", EvidenceClass.CO_OCCURRENCE)
        # Dated by the event (the transcript record that carried the model), never by the
        # bookkeeping, so the next seam check compares clocks honestly: a fresher heartbeat
        # stamp beats this one, an older tail read loses to it.
        await actions.assert_property(a, "source_model", identity.model, src,
                                      identity.model_observed_at or now,
                                      confidence_for(ec), evidence_class=ec.value)
    if identity.model_divergent and identity.model_declared:
        # The agent self-reported a model that disagrees with the harness: keep its word as
        # the weak signal it is. The mismatch with source_model (observed) is the flag.
        sr = EvidenceClass.CO_OCCURRENCE
        await actions.assert_property(a, "source_model_declared", identity.model_declared, src,
                                      now, confidence_for(sr), evidence_class=sr.value)
    if expected_model:
        # The swap detector: stamp the intent, and when the observed model diverges from it,
        # a silent danger-demotion by the harness, record the swap as a first-class observed
        # event (not the agent's self-report; it can't feel its own swap). Gate the swap on a
        # job_dir anchor: a cwd/self-report model may be a neighbor's, and a divergence
        # asserted off it is a false alarm; the true positive is the anchored read. The
        # repo's own declared intent (.osiris model=) outranks the machine-level default: a fleet of
        # onboarded repos does not all run the same model, and the operator's choice is
        # never a violation.
        expected_model = read_project_model(identity.cwd) or expected_model
        verdict = classify_swap(identity.model_history, identity.model, expected=expected_model,
                                anchored=identity.model_method == "job_dir",
                                deliberate=identity.model_deliberate)
        await actions.assert_property(a, "model_intent", expected_model, src, now, _CONF,
                                      evidence_class=_EC)
        if verdict.swapped and not verdict.deliberate:
            # Re-scoped per an operator distinction between an involuntary demotion and a
            # direct, deliberate model change: `model_swapped` is the exact property the
            # digest's danger map reads (sessions.py's own miner docstring), so stamping it
            # for a witnessed, deliberate model change is a false positive on that map,
            # indistinguishable from the harness's silent danger-demotion this property
            # exists to catch. The flag is for the harness changing the model without the
            # operator; an operator-initiated change is recorded durably below instead
            # (intended_model plus the pin), never as a danger sighting.
            do = EvidenceClass.DIRECT_OBSERVATION
            await actions.assert_property(a, "model_swapped", swap_marker(verdict), src, now,
                                          confidence_for(do), evidence_class=do.value)
        if verdict.deliberate and verdict.to_model:
            # The standing-choice write side: the operator's own model-change command on the
            # record is the operator re-pinning this seat's standing choice, so auto-stamp
            # intended_model so the choice persists across successions and relaunches with
            # no manual re-pinning. The read side already exists (mint_seat's own pin,
            # launch()'s precedence); this is the one write it was missing. Gated on
            # `deliberate` specifically, never `swapped` alone: only a witnessed model
            # transition sets it, never an involuntary demotion by the harness or a cold
            # divergence guessed from the intent alone.
            from src.orchestrator.seats import held_seat
            seat = await held_seat(actions.pool, identity.agent_id)
            if seat:
                soid = await actions.create_or_find_object("Seat", seat["seat_id"], src)
                await actions.assert_property(soid, "intended_model", verdict.to_model, src,
                                              now, _CONF, evidence_class=_EC)
                # The pin becomes a cache, not a competing claim: the graph stamp above is
                # durable but invisible to `_expected_model`'s first-checked source (the
                # .osiris file itself) and to a fresh launch on a machine that never talks to
                # this graph. Writing the file closes both gaps in one act; no other reader
                # needs to change, since read_project_model/_expected_model already check the
                # file before anything else.
                if seat.get("handle"):
                    await write_model_pin(str(seat["handle"]), verdict.to_model)
    # A rule for inferring "where this lineage's work actually landed", ported from
    # project_identity.py's own _write_attribution, the same query, reused rather than
    # re-derived. Bases = this agent's own lineage only (not a seat's holder history:
    # resolve_identity/register_agent run before any seat necessarily exists, so the agent's
    # own generation-stripped id is the one lineage key guaranteed on hand).
    #
    # The acceptance condition: if this inference picks a project, it is wrong, however good
    # the pick. This never overwrites `identity.project`; it reports agreement/disagreement
    # honestly and stops. A later, separately-scoped build decides whether or when this rule
    # gets to win a disagreement; this lane only makes the disagreement visible, which
    # nothing before it could do at all.
    #
    # Degrades, never blocks (this sits on the mount path every session in the fleet
    # traverses): a failed query here must never be the reason a mount fails. None/0/None
    # (the dataclass defaults) is an honest "could not determine", not a wrong answer.
    try:
        from src.orchestrator.project_identity import _write_attribution
        wa = await _write_attribution(actions.pool, [_generation(identity.agent_id)[0]])
    except Exception as exc:  # noqa: BLE001 : a DB hiccup on a diagnostic signal must
                               # never be the thing that blocks a mount
        logger.warning("write-attribution check failed for %s: %r", identity.agent_id, exc)
        wa = None
    if wa is not None:
        identity.write_attribution_top = wa["top"]
        identity.write_attribution_total = wa["total"]
        if wa["total"] == 0:
            identity.write_attribution_agreement = "no-signal"
        else:
            # Normalize through merged_into before comparing: `wa["top"]` is already the
            # survivor's live label (`_write_attribution` reads it off the live in_repo
            # edge), but `identity.project` is whatever the seat's own pin/cwd resolution
            # produced, which may still name a label that has since been folded into another
            # project. Comparing the raw strings false-fires "disagrees" on every folded seat
            # forever after its fold, even though both sides name the same entity. Degrades
            # to the raw (pre-fix) comparison on any failure: a diagnostic refinement must
            # never be the reason a mount fails.
            project_label = identity.project
            merge_confession: str | None = None
            if identity.project:
                try:
                    from src.orchestrator.project_identity import (
                        _normalize_project_label_through_merge,
                    )
                    project_label, merge_confession = (
                        await _normalize_project_label_through_merge(
                            actions.pool, identity.project))
                except Exception as exc:  # noqa: BLE001 : see note above
                    logger.warning("merge-normalization failed for %r: %r",
                                   identity.project, exc)
                    project_label = identity.project
            if wa["top"] == project_label:
                identity.write_attribution_agreement = "confirms"
            else:
                identity.write_attribution_agreement = "disagrees"
                # A disagreement is the actionable case: durable, so a later audit (or a
                # human skimming dossier()) can see it without having caught the live
                # mount() banner. Derived evidence (an inference from write history, not a
                # declaration): weaker than the self-declared properties around it, on
                # purpose.
                do_ec = EvidenceClass.DERIVED
                await actions.assert_property(
                    a, "write_attribution_disagreement",
                    f"lineage writes mostly to {wa['top']!r} "
                    f"({wa['breakdown'].get(wa['top'], 0)}/{wa['total']}) but this "
                    f"session resolved {identity.project!r}"
                    + (f" ({merge_confession})" if merge_confession else ""),
                    src, now, confidence_for(do_ec), evidence_class=do_ec.value)
    if identity.project:
        await actions.assert_property(a, "project", identity.project, src, now, _CONF,
                                      evidence_class=_EC)
        proj = await _resolve_or_mint_project(actions, identity.project, src)
        # The project-name clobber: this used to reassert `name` from the caller's own
        # pin/identity.project unconditionally, at the same self-declared confidence a
        # deliberate rename_project/correct_project_name write uses. current_assertions'
        # tie-break (confidence DESC, observed_at DESC) then falls through to pure recency,
        # so any later, uninformed mount silently overturns an earlier, reasoned rename.
        # Measured live: one project's declared name was reverted to its old name by several
        # ordinary mounts over a couple of days, a recurring failure mode through a far more
        # common trigger than a disk census: this line, on every mount of a seat with a
        # stale pin. Fix: only write at full confidence when there is no existing declared
        # name yet, or the difference is case/whitespace-only (an already-established safe
        # exception). A genuine difference is never silently dropped, still recorded so
        # nothing is hidden from history or from project_identity_evidence's own audit, but
        # at derived-tier confidence, so a routine, uninformed mount can never outrank a
        # declared rename on recency alone.
        if proj is not None:
            # The self-reinfecting fold: the clobber fix above downgrades a differing
            # pin-derived name to derived-tier confidence rather than refusing it outright,
            # the right instinct but the wrong mechanism. assert_property's supersession is
            # same-source-only, so a derived write from this session's source still lands as
            # a new current row beside the strong ones; it loses a confidence-ordered read
            # but wins a recency-ordered one. Measured live: one project carried a
            # direct-observation/0.9 name from an earlier fold, then a derived/0.4 row using
            # its pre-fold name was written days later by exactly this code path (a mount
            # resolving a stale .osiris pin that still declared the pre-fold name),
            # self-reinfecting on every such mount, never healed by a graph-only cleanup.
            # The structural fix: when the incoming label is provably a dead identity, the
            # canonical or a current `name` of some other SoftwareProject already
            # status='merged' into this exact `proj`, there is no genuine disagreement to
            # record at any confidence; it is a fold being partially undone. Skip the write
            # entirely, not merely downgrade it. This is a stronger signal than the
            # case/whitespace check above, checked first: a merge record is structural
            # proof, not a heuristic.
            dead_husk_name = await actions.pool.fetchval(
                "SELECT 1 FROM objects m WHERE m.type='SoftwareProject' "
                "AND m.status='merged' AND m.merged_into=$1 AND ("
                "  lower(m.canonical) = lower($2) OR EXISTS ("
                "    SELECT 1 FROM current_assertions ca WHERE ca.object_id=m.id "
                "    AND ca.name='name' AND lower(ca.value #>> '{}') = lower($3))) "
                "LIMIT 1",
                proj, f"repo:{identity.project}", identity.project)
            existing_row = await actions.pool.fetchrow(
                "SELECT value #>> '{}' AS name, confidence, source_id FROM current_assertions "
                "WHERE object_id=$1 AND name='name' AND source_id=$2 "
                "ORDER BY confidence DESC, observed_at DESC LIMIT 1", proj, src)
            top_row = await actions.pool.fetchrow(
                "SELECT value #>> '{}' AS name FROM current_assertions "
                "WHERE object_id=$1 AND name='name' "
                "ORDER BY confidence DESC, observed_at DESC LIMIT 1", proj)
            existing_name = top_row["name"] if top_row else None
            # The supersede-not-outrank gap, project identity drift: measured live, one
            # project was renamed at 0.95 confidence, then a mount less than two minutes
            # later from the same session re-derived the still-unmoved on-disk folder's
            # basename and wrote the old name back at derived/0.4 confidence. The comment
            # above already downgrades this write's own confidence so a cross-source
            # confidence-ordered read would still favor the rename, but assert_property's
            # supersession is same-source-only (by design; every other write in this
            # codebase depends on that), so a same-source write at any confidence supersedes
            # that source's own prior current row outright: the rename's own assertion
            # vanishes from current_assertions entirely, not merely loses a confidence
            # contest. Scoped to same-source only: a cross-source derived write never erases
            # anything (a different source's row is untouched by supersession), so the
            # existing "still recorded, just outranked" behavior for that case stays exactly
            # as it was. Extends the existing "skip the write entirely" structural fix
            # (dead_husk_name, above) to this second shape: this source's own prior current
            # row for this property already carries higher confidence than a derived write
            # would, so skip, never write-and-erase it.
            outranked = (existing_row is not None
                        and existing_row["confidence"] > confidence_for(EvidenceClass.DERIVED))
            if dead_husk_name:
                pass  # a folded husk's own name resurrected by a stale pin: never written
            elif (existing_name is None
                    or existing_name.strip().casefold() == identity.project.strip().casefold()):
                await actions.assert_property(proj, "name", identity.project, src, now, _CONF,
                                              evidence_class=_EC)
            elif outranked:
                pass  # a higher-confidence current name already stands (a deliberate
                # rename, most likely); writing even a derived-confidence value from this
                # same source would supersede and erase it, not merely lose a
                # confidence-ordered contest, so skip rather than clobber
            else:
                do = EvidenceClass.DERIVED
                await actions.assert_property(proj, "name", identity.project, src, now,
                                              confidence_for(do), evidence_class=do.value)
            already_works_in = await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
                "LIMIT 1", a, proj)
            if not already_works_in:
                await _flag_works_in_alongside_prior(
                    actions, a, proj, identity.project, src, now)
            await _link_once(actions, a, proj, "works_in", src, now)
    if identity.cwd:
        # The repo path, which lets the trigger hook resolve a project to where it wakes.
        await actions.assert_property(a, "cwd", identity.cwd, src, now, _CONF, evidence_class=_EC)
    principal = await actions.create_or_find_object("Person", f"principal:{actor}", src)
    await actions.assert_property(principal, "name", actor, src, now, _CONF, evidence_class=_EC)
    await _link_once(actions, a, principal, "acts_for", src, now)
    # The post-mint invariant: the Agent object mints under mint_lock above, but its
    # works_in link (just above, when identity.project resolves at all) is a separate later
    # write outside that lock; no single actions.atomic() block spans both, so an earlier
    # refuse-and-rollback gate can't reach across the gap. This never refuses: a
    # project-less mount (no cwd/seat ever resolved one) is a real, common, legitimate
    # state, not a bug. It just confesses that gap the same honest way a caller's own
    # unlinked_because would, once, idempotently, rather than leaving it silent. The
    # heartbeat sub-sweep (seats.py's post_mint_orphan_sweep) catches whatever a crash
    # between mint_lock and this line missed.
    from src.orchestrator.capture import confirm_or_confess_link
    await confirm_or_confess_link(
        actions, a, "works_in",
        reason="no live works_in link observed when register_agent's post-mint invariant ran",
        source=src, observed=now)
    if genuinely_fresh and identity.project:
        await _flag_unattributed_revisit(
            actions, base=_generation(src)[0], project=identity.project, src=src, now=now)
    return a


async def seat_bearings(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """Who am I, and whose job is vacant here? This closes a gap where the house/seat/holder
    model stamped a seat in the graph, but orient() went on answering with a bare agent id and
    nothing about the seat. The refusal to double-seat a house was fixed before this, but the
    discovery wasn't: an agent would not be refused as an unrecognized session anymore, but it
    would simply never learn that a named seat existed for it to claim. A fresh mind reads the
    briefing and nothing else, so an inheritance nobody is told about is not an inheritance. It
    protects a name the next holder will never reach for.

    So the briefing now says it: your seat if you hold one; and if you are anonymous, the seats
    of your house that are standing empty, with the verb that takes them."""
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
    # The binding is part of who you are: a mind that actively holds a Seat object is told
    # so, whether or not it ever claimed a name for itself in the assertion world. An
    # attached-at-birth mind that reads its own orient() and sees nothing was the discovery
    # gap all over again, one layer down.
    from src.orchestrator.seats import held_seat
    bound = await held_seat(pool, agent_id)
    binding = {"seat_binding": bound} if bound else {}
    # A seated mind's house is derived: held_seat already walked the managed_by chain to
    # compute `binding`'s own seat_binding.house; this function used to ignore that and read
    # the Agent's own raw `project` stamp instead (the same duplicate house_of() reads
    # independently), the exact bypass orient() shipped through. An unseated mind has no
    # seat to walk, so the raw stamp remains its only signal; that branch is unchanged.
    house = bound["house"] if bound and bound.get("house") else (seat["house"] if seat else None)
    if seat and seat["handle"]:
        gen = int(seat["gen"]) if seat["gen"] else None
        return {"seat": seat_label(agent_id, seat["handle"], gen), "house": house,
                **binding}

    if not house:
        return binding
    # Anonymous: what jobs does this house have, and is anyone sitting in them?
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
