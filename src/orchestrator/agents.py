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
from src.orchestrator.swaps import classify_swap
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
    declared = model  # the agent's SELF-REPORT of its model (may be None) — the WEAK signal
    observed: str | None = None
    method: str | None = None
    history: list[str] = []  # the transcript's model sequence — the swap history (job_dir path)
    if job_dir:
        observed, history, _ = current_model(root=root, job_dir=job_dir)  # the harness's record
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
    resolved = sid is not None
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


async def register_agent(
    actions: Actions, identity: AgentIdentity, *, actor: str, expected_model: str | None = None
) -> uuid.UUID:
    """Mint (idempotently) the Agent object + its org-chart links. The agent attributes
    its OWN registration (`source = agent:<session>`), SELF_DECLARED. Re-mount is a no-op
    (find-or-create + the kernel's byte-dup assertion skip absorb it). `expected_model` (the
    operator's standing choice) turns on the swap-detector: the intent is stamped, and a silent
    demotion away from it is recorded as a first-class OBSERVED event on the Agent."""
    now = datetime.now(UTC)
    src = identity.agent_id
    a = await actions.create_or_find_object("Agent", identity.agent_id, src)
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
        verdict = classify_swap(identity.model_history, identity.model, expected=expected_model)
        await actions.assert_property(a, "model_intent", expected_model, src, now, _CONF,
                                      evidence_class=_EC)
        if verdict.swapped:
            do = EvidenceClass.DIRECT_OBSERVATION
            marker = f"{verdict.from_model} → {verdict.to_model}"
            await actions.assert_property(a, "model_swapped", marker, src, now,
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
