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

from src.actions.core import Actions
from src.ingest.sessions import (
    _job_id,
    _tail_lines,
    current_model,
    latest_model,
    locate_transcript_by_cwd,
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


def resolve_identity(
    *, cwd: str | None = None, job_dir: str | None = None,
    session: str | None = None, model: str | None = None, root: Path | None = None,
) -> AgentIdentity:
    """Resolve an agent's identity from what it can tell the server + what the harness RECORDS.
    The project comes from its cwd; the session + model are OBSERVED off its own transcript. Two
    probe paths: the CLAUDE_JOB_DIR anchor (precise), or — when absent — the cwd's project dir,
    whose newest transcript is the active session. Ruling 17516660: OBSERVATION outranks the
    agent's self-report (the harness doesn't lie; a swap is below the agent's own horizon), so a
    passed `model` is used only when nothing can be observed, and a passed model that DISAGREES
    with the observation is kept as `model_declared` + flagged `model_divergent`. `root` overrides
    the transcript search dir (tests inject a tmp root; production reads ~/.claude/projects)."""
    project = Path(cwd).name if cwd else None
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
        observed, history, _ = current_model(  # the harness's record — anchored to THIS session
            root=root, job_dir=job_dir, anchored_only=True)
        if observed is not None:
            method = "job_dir"
    if sid is None and cwd:  # no job dir → find the session (and, if unseen, the model) by cwd
        path = locate_transcript_by_cwd(cwd, root=root)
        if path is not None:
            sid = path.stem.split("-")[0]  # the 8-char handle, matching the job-id scheme
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
    actions: Actions, identity: AgentIdentity, *, actor: str, expected_model: str | None = None
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
    anchored source_model: a disagreement the CURRENT transcript can't explain (the prior model
    is nowhere in its history — this context never was that model) is stamped `model_succession`
    ("<prior> → <observed>", DIRECT_OBSERVATION) and echoed on `identity.model_succession` so
    mount() can confess it. A transition the transcript DID witness stays the warm-swap's
    (`model_swapped`) — same context, different seam."""
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
        # the succession seam: read the baseline BEFORE the new observation supersedes it
        prior = await _last_anchored_model(actions, a)
        if (prior is not None and prior != identity.model
                and prior not in identity.model_history):
            identity.model_succession = f"{prior} → {identity.model}"
            mint_because = mint_because or "model-succession"
    if mint_because:
        heir = next_generation(identity.agent_id)
        ancestor_id, ancestor_oid = identity.agent_id, a
        identity.succeeded_from = ancestor_id
        identity.agent_id = heir
        src = heir
        a = await actions.create_or_find_object("Agent", heir, src)
        do = EvidenceClass.DIRECT_OBSERVATION
        await actions.assert_property(a, "succeeded_from", ancestor_id, src, now,
                                      confidence_for(do), evidence_class=do.value)
        await actions.assert_property(a, "minted_because", mint_because, src, now,
                                      confidence_for(do), evidence_class=do.value)
        if identity.model_succession:
            await actions.assert_property(a, "model_succession", identity.model_succession,
                                          src, now, confidence_for(do), evidence_class=do.value)
        # the forward pointer the head-walk follows, and the graph edge heirs are read by
        await actions.assert_property(ancestor_oid, "succeeded_by", heir, src, now,
                                      confidence_for(do), evidence_class=do.value)
        await _link_once(actions, a, ancestor_oid, "succeeded_from", src, now)
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
