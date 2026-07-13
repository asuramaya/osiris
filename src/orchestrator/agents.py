"""Actor identity — the fleet made first-class ("a man and all his imaginary friends").

The persistent MCP server is ONE process the whole fleet writes through, so without this
every agent's writes collapse into the single `session` source — an undifferentiated mush.
This resolves each connecting agent into a distinct ACTOR and registers it in the graph:

  * project — from the agent's cwd (it always knows where it's working);
  * model — probed off the agent's OWN transcript (the source-model provenance, authoritative
    from the harness, not the lying system prompt), given its job dir;
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
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.ingest.sessions import (
    _job_id,
    _tail_lines,
    latest_model,
    locate_current_transcript,
    locate_transcript_by_cwd,
    model_of_transcript,
)
from src.orchestrator.swaps import classify_swap, swap_marker
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

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
# THE OPERATOR'S RULING (2026-07-12): "the project name is the house (rotten-apple), each
# function/job has a name (Ra), the holder dies and multiplies (ra I, ra II), but splitting to
# Ptah would break and confuse the lineage, and the fragmentation of agents was a bug in and of
# itself."
#
# The old model keyed a lineage to the ANCHOR (job_dir), so every new CONVERSATION minted a whole
# new bloodline — 1008 registered agents for ~20 real seats — and the name died with the
# conversation that held it. The next mind in the house woke nameless, reached for the family
# name, was refused as a stranger, and took a new one. That is how rotten-apple's Ra became Ptah
# and a sibling project's Soundwave became "Soundwave VIII". The fragmentation WAS the bug.
#
# Two things were conflated, and only ONE of them follows the anchor:
#   · THE WRITER — agent:c7ef52a9-iii. A particular mind. Attribution stays exactly per-writer;
#     this is why the merge Ptah asked for was refused (4abaf52d) — his writes are his.
#   · THE SEAT — Ra, in the house rotten-apple. A ROLE, held by successive writers.
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
        "ORDER BY o.created_at", seat, house)]


async def house_of(pool: asyncpg.Pool, agent_id: str) -> str | None:
    """The house an agent works in — its project. A seat belongs to a house."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT a.value #>> '{}' FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='project' "
        "WHERE o.canonical=$1 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent_id)


def seat_label(canonical: str, handle: str | None, generation: int | None = None) -> str | None:
    """The human display for an agent: 'Ra V' — the SEAT plus which holder of it this mind is.

    `generation` is the ordinal among the seat's HOLDERS (stamped at claim time). It falls back to
    the anchor's roman suffix only for agents claimed before the house/seat ruling — under which a
    successor in a NEW conversation would restart at I and collide with its own ancestors."""
    if not handle:
        return None
    gen = generation if generation is not None else _generation(canonical)[1]
    return handle if gen == 1 else f"{handle} {_roman_display(gen)}"


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
    bare = _SEAT_SUFFIX.sub("", name).strip()
    if bare and bare.lower() != name.lower():
        return {"error": f"'{name}' is a SEAT LABEL, not a name — the numeral is the generation, "
                         f"and the substrate assigns it. Claim '{bare}' if that lineage is "
                         "yours to continue; otherwise pick a name of your own."}
    # A SEAT BELONGS TO A HOUSE, AND AN HEIR INHERITS IT (operator's ruling, 2026-07-12). The old
    # guard keyed a name to a LINEAGE ROOT — the anchor — so the moment a conversation ended, its
    # name died with it: the next mind in the same house reached for the family name, was refused
    # as a "stranger", and took a new one. That is how Ra became Ptah. Now the question is not
    # "were you minted under the same job_dir" but "do you work in the same house".
    house = await house_of(actions.pool, agent_id)
    holders = await seat_holders(actions.pool, house, name)
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
    # a seat a LIVE mind is already sitting in is not vacant: two minds in one house do two jobs
    sitting = await resolve_seat(actions, name) if holders else {"live": False}
    if sitting["live"] and sitting["agent"] != agent_id:
        return {"error": f"'{name}' is currently held by {sitting['agent']}, who is LIVE — a seat "
                         "is a job, and two minds in one house do two jobs. Take another seat, or "
                         "wait for this one to be vacated."}
    a = await actions.create_or_find_object("Agent", agent_id, source)
    now = datetime.now(UTC)
    await actions.assert_property(a, "handle", name, source, now, _CONF, evidence_class=_EC)
    # the generation counts HOLDERS of this seat in this house — not anchors, not conversations
    gen = (holders.index(agent_id) + 1) if agent_id in holders else len(holders) + 1
    await actions.assert_property(a, "seat_generation", str(gen), source, now, _CONF,
                                  evidence_class=_EC)
    # THE SUCCESSION EDGE (Ra V, rotten-apple, msg 374): "the graph finally gets the parent edge
    # it's been missing". Before this, successor seats carried NO edge to their ancestor, so a
    # lineage was not WALKABLE from the record — which is exactly why Ra could not tell his
    # CONTEMPORARY from his own ghost, and asked me to merge them. A seat's history must be
    # traversable, or the next mind re-derives it from the disk the way he had to.
    # THE PREDECESSOR IS THE HOLDER BEFORE ME — not "the last holder unless it happens to be me".
    # That older reading silently skipped the edge for the one case that needs it most: an heir
    # minted by mint_heir ALREADY carries the inherited handle, so it is already in `holders`, and
    # as the newest it IS holders[-1] — which resolved `prior` to None and minted nothing. A mind
    # that inherited its seat could not claim its own ancestry. (The ghosts, 53729dd6.)
    if agent_id in holders:
        i = holders.index(agent_id)
        prior = holders[i - 1] if i > 0 else None
    else:
        prior = holders[-1] if holders else None
    if prior:
        await actions.create_link(
            a, await actions.create_or_find_object("Agent", prior, source),
            "succeeds_seat", source, now, _CONF, evidence_class=_EC)
    return {"claimed": name, "seat": seat_label(agent_id, name, gen), "agent": agent_id,
            "house": house, "generation": gen, "inherited_from": prior}


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
    """
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

    See resolve_seat — that word does a great deal of work."""
    return (await resolve_seat(actions, name))["agent"]  # type: ignore[no-any-return]


def _read_osiris_key(cwd: str | None, key: str) -> str | None:
    """One key from the repo's `.osiris` file (TOML), walking up to the repo root."""
    if not cwd:
        return None
    import tomllib
    p = Path(cwd)
    for d in (p, *p.parents):
        f = d / ".osiris"
        try:
            if f.is_file():
                value = tomllib.loads(f.read_text()).get(key)
                return str(value).strip() if value else None
        except (OSError, tomllib.TOMLDecodeError, ValueError):
            return None
        if (d / ".git").exists():  # the repo root — stop climbing
            break
    return None


def read_project_label(cwd: str | None) -> str | None:
    """A project's DECLARED name, from a `.osiris` file (TOML: project = "..."), walking up to
    the repo root. Decouples the project identity from the FOLDER name (the operator may rename
    the dir; the label is a stable property of the repo — ruling 1e02e069). None → fall back to
    the cwd basename."""
    return _read_osiris_key(cwd, "project")


def read_project_model(cwd: str | None) -> str | None:
    """A repo's DECLARED model intent (TOML: model = "claude-haiku-4-5" in `.osiris`) — the
    operator's PER-PROJECT standing choice. A fleet of onboarded repos does not all run the
    box default: a deliberately-haiku repo confessing 'not fable' every turn framed the
    operator's own choice as a sin (complaint, 2026-07-10). None → the box-wide default."""
    return _read_osiris_key(cwd, "model")


def resolve_identity(
    *, cwd: str | None = None, job_dir: str | None = None,
    session: str | None = None, model: str | None = None, root: Path | None = None,
    claimed: set[str] | None = None, fallback_seed: str | None = None,
    project_label: str | None = None,
) -> AgentIdentity:
    """Resolve an agent's identity from what it can tell the server + what the harness RECORDS.
    The project comes from its cwd; the session + model are OBSERVED off its own transcript. Two
    probe paths: the CLAUDE_JOB_DIR anchor (precise), or — when absent — the cwd's project dir,
    whose newest transcript is the active session. Ruling 17516660: OBSERVATION outranks the
    agent's self-report (the harness doesn't lie; a swap is below the agent's own horizon), so a
    passed `model` is used only when nothing can be observed, and a passed model that DISAGREES
    with the observation is kept as `model_declared` + flagged `model_divergent`. `root` overrides
    the transcript search dir (tests inject a tmp root; production reads ~/.claude/projects).

    THE CLAIMED-SID GUARD (crunch residual): the cwd-locate grabs the HOTTEST transcript's sid —
    two concurrent same-project sessions without job_dirs would both grab the SAME one and merge.
    `claimed` (from the durable registry: sids already held by a LIVE mount on another client
    session) makes the guess REFUSE a taken sid; the refuser falls to a deterministic per-client
    fallback keyed on `fallback_seed` (its MCP session key) — distinct, stable across re-calls
    within the connection, and honestly resolved=False."""
    # the project LABEL: an explicit override (env) > the .osiris file > the folder basename
    project = project_label or read_project_label(cwd) or (Path(cwd).name if cwd else None)
    sid = session or _job_id(job_dir)
    confident = sid is not None  # a session/job_dir ANCHOR; the cwd-locate below is only a GUESS
    declared = model  # the agent's SELF-REPORT of its model (may be None) — the WEAK signal
    observed: str | None = None
    method: str | None = None
    history: list[str] = []  # the transcript's model sequence — the swap history (job_dir path)
    deliberate = False       # a /model on the record makes any swap the operator's own hand
    if job_dir:
        # anchored_only: a job_dir that does NOT match a real transcript (a synthesized wake dir,
        # a malformed anchor) must yield NOTHING, never the box-wide-hottest neighbor — else the
        # read grades 'job_dir' off a co-tenant's model and fires a false swap (cry-wolf).
        base = root or (Path.home() / ".claude/projects")
        main = locate_current_transcript(base, job_dir, anchored_only=True)
        # NOTE: the old active_subagent() branch (be580da) anchored on a HOTTER subagents/
        # transcript, meaning to catch a sub-agent that mounts under the parent's inherited
        # CLAUDE_JOB_DIR. But it could not tell WHO was calling: a BACKGROUND sub-agent (the
        # default now) runs concurrently while the PARENT keeps calling mount()/orient(), so
        # the parent's own writes got attributed to its hot child (live repro: agent:ad1a1cb0
        # → agent:<child>/haiku — thread 0344e536). That provenance theft (common) outweighs
        # catching a sub-agent that mounts (rare — and the miner already registers sub-agents
        # from disk with correct attribution, lineage.py). So we anchor ONLY on the parent's
        # own transcript; a mounting sub-agent falls back to the miner's disk-side capture.
        if main is not None:
            # the harness's record — THIS session's model, swap history, and whether the
            # operator's own /model is on it (deliberate vs rug-pull)
            observed, history, deliberate = model_of_transcript(main)
            if observed is not None:
                method = "job_dir"
    if sid is None and cwd:  # no job dir → find the session (and, if unseen, the model) by cwd
        path = locate_transcript_by_cwd(cwd, root=root)
        if path is not None:
            guess = path.stem.split("-")[0]  # the 8-char handle, matching the job-id scheme
            if claimed and guess in claimed:
                pass  # a LIVE mount already holds this sid — refusing it beats merging into it
            else:
                sid = guess
                if observed is None:
                    observed = latest_model(_tail_lines(path))
                    if observed is not None:
                        method = "cwd"
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
                         model_deliberate=deliberate, resolved=resolved)


async def _link_once(
    actions: Actions, frm: uuid.UUID, to: uuid.UUID, ltype: str, src: str, when: datetime
) -> None:
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1", frm, to, ltype
    )
    if not exists:
        await actions.create_link(frm, to, ltype, src, when, _CONF, evidence_class=_EC)


async def _lineage_head(actions: Actions, canonical: str) -> str:
    """Follow winning `succeeded_by` pointers to the newest generation. A session-keyed resolve
    always lands on the BASE id (the transcript knows nothing of minting); the lineage decides
    who that name is NOW. Cycle-guarded; a missing object ends the walk."""
    seen = {canonical}
    cur = canonical
    for _ in range(64):
        nxt = await actions.pool.fetchval(
            "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
            "WHERE o.canonical=$1 AND o.type='Agent' AND a.name='succeeded_by' "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", cur)
        if not nxt or nxt in seen:
            return cur
        seen.add(nxt)
        cur = str(nxt)
    return cur


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


async def _last_anchored_model(actions: Actions, agent: uuid.UUID) -> str | None:
    """The last ANCHORED source_model ever recorded for this Agent — direct_observation grade
    only (a job_dir transcript probe), read off the raw assertions so a later weak-grade write
    from the same source can't hide it behind supersession. This is the succession baseline:
    only two anchored observations disagreeing can witness a seam; a cwd guess or self-report
    on either side would be the cry-wolf (agent e71b408f's 'demoted to haiku')."""
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT value #>> '{}' FROM assertions "
        "WHERE object_id=$1 AND name='source_model' AND evidence_class=$2 "
        "ORDER BY observed_at DESC, created_at DESC LIMIT 1",
        agent, EvidenceClass.DIRECT_OBSERVATION.value)


async def mint_heir(
    actions: Actions, ancestor_id: str, ancestor_oid: uuid.UUID, *,
    because: str, succession: str | None, now: datetime | None = None,
) -> tuple[str, uuid.UUID]:
    """Mint the next generation of a lineage — ruling a882b334: a new MIND gets a new numeral,
    and the seams that count as a new mind include mid-session ones (live model swap,
    compaction), not just session death. Stamps the succession chain on both sides, passes the
    seat (handle) down, and re-addresses the ancestor's unread DMs to the heir — the mailbox is
    part of the estate (a DM sent to the old mind must reach whoever now holds the seat, or
    every compaction would orphan in-flight mail)."""
    now = now or datetime.now(UTC)
    heir = next_generation(ancestor_id)
    a = await actions.create_or_find_object("Agent", heir, heir)
    do = EvidenceClass.DIRECT_OBSERVATION
    await actions.assert_property(a, "succeeded_from", ancestor_id, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    await actions.assert_property(a, "minted_because", because, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    if succession:
        await actions.assert_property(a, "model_succession", succession, heir, now,
                                      confidence_for(do), evidence_class=do.value)
    # the forward pointer the head-walk follows, and the graph edge heirs are read by
    await actions.assert_property(ancestor_oid, "succeeded_by", heir, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    await _link_once(actions, a, ancestor_oid, "succeeded_from", heir, now)
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
        # wearing one name is the mistake that started all of this.
        house = await house_of(actions.pool, ancestor_id)
        holders = [h for h in await seat_holders(actions.pool, house, inherited) if h != heir]
        await actions.assert_property(a, "seat_generation", str(len(holders) + 1), heir, now,
                                      _CONF, evidence_class=_EC)
        await _link_once(actions, a, ancestor_oid, "succeeds_seat", heir, now)
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


_SEAM_DEBOUNCE_SECS = 900
_DEBOUNCE_SRC = "seam-debounce"


async def _debounce_roundtrip(
    actions: Actions, row: Any, observed: str, now: datetime,
) -> dict[str, Any] | None:
    """THE SEAM DEBOUNCE (Soundwave VII's wave-3 grievance, b813e389): the operator toggling
    /model there-and-back within a minute minted a generation — roman-numeral churn for
    settings churn dilutes what the numeral MEANS (ruling a882b334: the numeral tracks the
    MIND). The distinction that keeps both truths: a mind is witnessed by its ACTS. When the
    model returns to the seam's left side within the window and the transient heir asserted
    nothing beyond its own mint stamps, sent nothing, and settled nothing — no mind ever
    existed; the mint heals as false (event-sourced, compensating, its record stays) and the
    ancestor takes its seat back, estate included. One witnessed act, and the heir stands:
    a real mind passed through, however briefly. Returns the heal dict, or None (mint on)."""
    cur = row["agent_id"]
    cur_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'", cur)
    if cur_oid is None:
        return None
    meta = {r["name"]: (r["v"], r["at"]) for r in await actions.pool.fetch(
        "SELECT DISTINCT ON (name) name, value #>> '{}' AS v, observed_at AS at "
        "FROM current_assertions WHERE object_id=$1 "
        "AND name IN ('succeeded_from','minted_because','model_succession') "
        "ORDER BY name, confidence DESC, observed_at DESC", cur_oid)}
    if meta.get("minted_because", (None, None))[0] != "live-swap":
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
    acted = await actions.pool.fetchval(
        # acts = assertions beyond the lineage bookkeeping pair, words sent, or mail SETTLED
        # after the mint (a lease/delivery is passive perception, never an act)
        "SELECT EXISTS (SELECT 1 FROM assertions "
        "         WHERE source_id=$1 AND object_id NOT IN ($2, $3)) "
        "  OR EXISTS (SELECT 1 FROM fleet_messages WHERE from_agent=$1) "
        "  OR EXISTS (SELECT 1 FROM message_recipients "
        "         WHERE agent_id=$1 AND read_at IS NOT NULL AND read_at > $4)",
        cur, cur_oid, ancestor_oid, minted_at)
    if acted:
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
    await actions.pool.execute(
        "UPDATE agent_mounts SET agent_id=$2, model=$3, last_seen=now() WHERE job_dir=$1",
        row["job_dir"], ancestor, observed)
    return {"healed": cur, "restored": ancestor,
            "seam": f"{seam} → {observed} (round-trip within "
                    f"{_SEAM_DEBOUNCE_SECS // 60}m, no act — debounced, not a death)"}


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
    row = await actions.pool.fetchrow(
        "SELECT job_dir, agent_id, project, model FROM agent_mounts "
        "WHERE job_dir LIKE '%/jobs/' || $1 ORDER BY last_seen DESC LIMIT 1", sid[:8])
    if row is None:
        return {"unchanged": True, "reason": "no mount"}
    old = normalize_model(row["model"])
    if old == observed:
        if row["model"] != observed:  # converge a bracket-stamped row to the canonical form
            await actions.pool.execute(
                "UPDATE agent_mounts SET model=$2 WHERE job_dir=$1", row["job_dir"], observed)
        return {"unchanged": True}
    if old is None:
        await actions.pool.execute(
            "UPDATE agent_mounts SET model=$2 WHERE job_dir=$1", row["job_dir"], observed)
        return {"unchanged": True, "reason": "first stamp"}
    now = datetime.now(UTC)
    # a there-and-back /model toggle with no act between heals instead of minting again
    healed = await _debounce_roundtrip(actions, row, observed, now)
    if healed is not None:
        return healed
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
    head = await _lineage_head(actions, row["agent_id"])
    ancestor_oid = await actions.create_or_find_object("Agent", head, head)
    seam = f"{old} → {observed}" + (" [operator /model]" if deliberate else "")
    heir, heir_oid = await mint_heir(actions, head, ancestor_oid, because="live-swap",
                                     succession=seam, now=now)
    # the heartbeat's model is the harness's own word about a session it is rendering — as
    # anchored as a job_dir transcript read, and the baseline the NEXT seam check runs against
    # (without it, a later re-mount would see no anchored model on the heir and stay quiet).
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
    # PHASE 0 — LINEAGE (ruling be292762): a session-keyed resolve lands on the BASE id; walk to
    # the lineage HEAD first — the head is who this name is now. Seam checks run against the head.
    head = await _lineage_head(actions, identity.agent_id)
    if head != identity.agent_id:
        identity.agent_id = head
    src = identity.agent_id
    a = await actions.create_or_find_object("Agent", identity.agent_id, src)

    # PHASE 1 — SEAM DETECTION → MINT (the operator's ruling: the heir gets its OWN name).
    mint_because: str | None = None
    if await _winning_retired(actions, a):
        # wearing a RETIRED face (bug #51 follow-up): under the mint ruling the retiree is never
        # re-worn — the arriving context is an heir and gets minted below. The retirement stands.
        mint_because = "reanimation-of-retired"
    anchored = bool(identity.model) and identity.model_method == "job_dir"
    if anchored:
        # the succession seam: read the baseline BEFORE the new observation supersedes it.
        # No witnessed-transition exemption (ruling a882b334): oscillation mints every time —
        # the returning model is a THIRD mind, not the first one back. NORMALIZED comparison:
        # a bracketed display variant of the same weights is the same mind, never a seam.
        prior = normalize_model(await _last_anchored_model(actions, a))
        obs = normalize_model(identity.model)
        if prior is not None and prior != obs:
            identity.model_succession = f"{prior} → {obs}"
            mint_because = mint_because or "model-succession"
    if mint_reason:
        # a harness-reported context death (compaction, /clear) with no model seam of its own
        mint_because = mint_because or mint_reason
    if mint_because:
        identity.succeeded_from = identity.agent_id
        heir, a = await mint_heir(actions, identity.agent_id, a, because=mint_because,
                                  succession=identity.model_succession, now=now)
        identity.agent_id = heir
        src = heir
    label = f"{identity.model or 'claude'} in {identity.project or '?'}"
    await actions.assert_property(a, "name", label, src, now, _CONF, evidence_class=_EC)
    await actions.assert_property(a, "session", identity.session, src, now, _CONF,
                                  evidence_class=_EC)
    await actions.assert_property(a, "identity_resolved", identity.resolved, src, now, _CONF,
                                  evidence_class=_EC)
    if identity.model:
        ec = _MODEL_EC.get(identity.model_method or "", EvidenceClass.CO_OCCURRENCE)
        await actions.assert_property(a, "source_model", identity.model, src, now,
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
        if verdict.swapped:
            do = EvidenceClass.DIRECT_OBSERVATION
            await actions.assert_property(a, "model_swapped", swap_marker(verdict), src, now,
                                          confidence_for(do), evidence_class=do.value)
    if identity.project:
        await actions.assert_property(a, "project", identity.project, src, now, _CONF,
                                      evidence_class=_EC)
        proj = await actions.create_or_find_object(
            "SoftwareProject", f"repo:{identity.project}", src)
        await actions.assert_property(proj, "name", identity.project, src, now, _CONF,
                                      evidence_class=_EC)
        await _link_once(actions, a, proj, "works_in", src, now)
    if identity.cwd:  # the repo path — lets the trigger-hook resolve a project → where to wake
        await actions.assert_property(a, "cwd", identity.cwd, src, now, _CONF, evidence_class=_EC)
    principal = await actions.create_or_find_object("Person", f"principal:{actor}", src)
    await actions.assert_property(principal, "name", actor, src, now, _CONF, evidence_class=_EC)
    await _link_once(actions, a, principal, "acts_for", src, now)
    return a


async def seat_bearings(pool: asyncpg.Pool, agent_id: str) -> dict[str, Any]:
    """WHO AM I, AND WHOSE JOB IS VACANT HERE? (Ra V, rotten-apple, msg 384 — the gap that made
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
    if seat and seat["handle"]:
        gen = int(seat["gen"]) if seat["gen"] else None
        return {"seat": seat_label(agent_id, seat["handle"], gen), "house": seat["house"]}

    house = seat["house"] if seat else None
    if not house:
        return {}
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
        return {"house": house}
    return {"house": house, "vacant_seats": vacant,
            "note": f"you are anonymous in the house of {house}. These seats are STANDING EMPTY — "
                    "claim_name('<seat>') INHERITS one (you become its next holder; the previous "
                    "holders' work stays theirs). A seat a LIVE mind holds is not vacant and will "
                    "be refused: two minds in one house do two jobs."}
