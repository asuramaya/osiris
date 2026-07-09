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
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
    # False when identity fell back to a best-effort id (no session/job-id/transcript anchor). The
    # fallback is now DISTINCT per session — never the old shared `agent:unknown` sink — so distinct
    # actors can't merge; the flag lets the fleet digest surface an unresolved onboarding.
    resolved: bool = True
    # SET BY register_agent (the one out-param): "<prior> → <observed>" when this registration
    # crossed a succession seam — a fresh context inheriting an identity another model wrote under
    # (bug #51, decepticons). None when no seam fired. mount() reads it to confess the seam.
    model_succession: str | None = None
    # SET BY register_agent: True when this mount RE-ATTACHED an identity carrying a winning
    # retired=true (bug #51 follow-up, decepticons msg 69). The trigger already refuses to
    # reanimate the retired (resume-not-mint), but a plain mount from the same session UUID would
    # silently un-retire the name. register_agent now stamps the reanimation as a first-class
    # OBSERVED event and mount() confesses it — never a silent reanimation (membrane, rule #6).
    reanimated: bool = False
    # SET BY register_agent under the MINT ruling (be292762): this context was minted a NEW
    # lineage-linked id (agent:<base>-ii…) because it arrived across a detected seam or wore a
    # retired face. Holds the ANCESTOR's canonical; mount() confesses the minting to the heir.
    succeeded_from: str | None = None


# Roman generations for successor ids (heinrich's grammar: agent:a8c15486-ii). The alphabet is
# DELIBERATELY restricted to {i, v, x} — none of which are hex digits — so a full-UUID canonical
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


def seat_label(canonical: str, handle: str | None) -> str | None:
    """The human display for an agent: 'Anna III' — the handle plus its lineage generation.
    None when the agent is still anonymous (unclaimed). Generation 1 shows the bare name."""
    if not handle:
        return None
    _, gen = _generation(canonical)
    return handle if gen == 1 else f"{handle} {_roman_display(gen)}"


async def claim_name(actions: Actions, agent_id: str, name: str, *, source: str) -> dict[str, Any]:
    """An agent names itself (ruling 1e02e069): the intelligence picks a meaningful name, the
    substrate enforces uniqueness. Refuses a name held by a DIFFERENT lineage (permanent
    exhaustion — a name belongs to one lineage forever; a successor inherits it automatically,
    a stranger cannot take it). Global namespace → unambiguous addressing. Stamps `handle` on
    the agent's Agent object (SELF_DECLARED)."""
    name = (name or "").strip()
    if not name or name.lower().startswith("agent:") or len(name) > 40:
        return {"error": "pick a short human name (not an id)"}
    root, _ = _generation(agent_id)
    holder = await actions.pool.fetchrow(
        "SELECT o.canonical FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "AND a.name='handle' WHERE o.type='Agent' AND lower(a.value#>>'{}')=lower($1) LIMIT 1",
        name)
    if holder is not None and _generation(holder["canonical"])[0] != root:
        return {"error": f"'{name}' is taken by {holder['canonical']} — a name belongs to one "
                         "lineage; pick another"}
    a = await actions.create_or_find_object("Agent", agent_id, source)
    await actions.assert_property(a, "handle", name, source, datetime.now(UTC), _CONF,
                                  evidence_class=_EC)
    return {"claimed": name, "seat": seat_label(agent_id, name), "agent": agent_id}


async def resolve_handle(actions: Actions, name: str) -> str | None:
    """A human name → the current holder's agent_id (the most-recently-active agent bearing it —
    the live generation of the seat). None if no agent holds it. Used to route a DM by name."""
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.canonical FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='handle' "
        "LEFT JOIN agent_mounts m ON m.agent_id=o.canonical "
        "WHERE o.type='Agent' AND lower(a.value#>>'{}')=lower($1) "
        "ORDER BY m.last_seen DESC NULLS LAST LIMIT 1", name)


def read_project_label(cwd: str | None) -> str | None:
    """A project's DECLARED name, from a `.osiris` file (TOML: project = "..."), walking up to
    the repo root. Decouples the project identity from the FOLDER name (the operator may rename
    the dir; the label is a stable property of the repo — ruling 1e02e069). None → fall back to
    the cwd basename."""
    if not cwd:
        return None
    import tomllib
    p = Path(cwd)
    for d in (p, *p.parents):
        f = d / ".osiris"
        try:
            if f.is_file():
                label = tomllib.loads(f.read_text()).get("project")
                return str(label).strip() if label else None
        except (OSError, tomllib.TOMLDecodeError, ValueError):
            return None
        if (d / ".git").exists():  # the repo root — stop climbing
            break
    return None


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
            observed, history = model_of_transcript(main)  # the harness's record — THIS session
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
                         model_divergent=divergent, model_history=tuple(history), resolved=resolved)


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
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='handle'",
        ancestor_oid)
    if inherited:
        await actions.assert_property(a, "handle", inherited, heir, now, _CONF,
                                      evidence_class=_EC)
    await actions.pool.execute(
        "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
        heir, ancestor_id)
    return heir, a


async def live_succession(
    actions: Actions, *, session_id: str, observed_model: str,
) -> dict[str, Any]:
    """A mid-session model change, sensed by the chrome heartbeat (ruling a882b334): the mind
    changed under a LIVE tab, so the seat passes now — mint the heir, move the durable mount
    row, and every per-render read (statusline, stop hook, digest) resolves to the new mind
    from the next glance. Idempotent: an unchanged model or an unknown mount is a no-op; a row
    with no stored model gets a first stamp, not a funeral (you can only die if you lived)."""
    sid = (session_id or "").strip().lower()
    if len(sid) < 8 or not observed_model:
        return {"unchanged": True, "reason": "no anchor"}
    row = await actions.pool.fetchrow(
        "SELECT job_dir, agent_id, project, model FROM agent_mounts "
        "WHERE job_dir LIKE '%/jobs/' || $1 ORDER BY last_seen DESC LIMIT 1", sid[:8])
    if row is None:
        return {"unchanged": True, "reason": "no mount"}
    old = row["model"]
    if old == observed_model:
        return {"unchanged": True}
    if old is None:
        await actions.pool.execute(
            "UPDATE agent_mounts SET model=$2 WHERE job_dir=$1", row["job_dir"], observed_model)
        return {"unchanged": True, "reason": "first stamp"}
    now = datetime.now(UTC)
    head = await _lineage_head(actions, row["agent_id"])
    ancestor_oid = await actions.create_or_find_object("Agent", head, head)
    heir, heir_oid = await mint_heir(actions, head, ancestor_oid, because="live-swap",
                                     succession=f"{old} → {observed_model}", now=now)
    # the heartbeat's model is the harness's own word about a session it is rendering — as
    # anchored as a job_dir transcript read, and the baseline the NEXT seam check runs against
    # (without it, a later re-mount would see no anchored model on the heir and stay quiet).
    do = EvidenceClass.DIRECT_OBSERVATION
    await actions.assert_property(heir_oid, "source_model", observed_model, heir, now,
                                  confidence_for(do), evidence_class=do.value)
    if row["project"]:
        await actions.assert_property(heir_oid, "project", row["project"], heir, now, _CONF,
                                      evidence_class=_EC)
    sid_prop = _job_id(row["job_dir"]) or sid[:8]
    await actions.assert_property(heir_oid, "session", sid_prop, heir, now, _CONF,
                                  evidence_class=_EC)
    await actions.pool.execute(
        "UPDATE agent_mounts SET agent_id=$2, model=$3, last_seen=now() WHERE job_dir=$1",
        row["job_dir"], heir, observed_model)
    handle = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions WHERE object_id=$1 AND name='handle'",
        heir_oid)
    return {"minted": heir, "from": head, "succession": f"{old} → {observed_model}",
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

    THE SUCCESSION SEAM (bug #51, decepticons): session-keyed identity means a retire+compact+
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
        # the returning model is a THIRD mind, not the first one back.
        prior = await _last_anchored_model(actions, a)
        if prior is not None and prior != identity.model:
            identity.model_succession = f"{prior} → {identity.model}"
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
        verdict = classify_swap(identity.model_history, identity.model, expected=expected_model,
                                anchored=identity.model_method == "job_dir")
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
