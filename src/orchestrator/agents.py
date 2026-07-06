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


def resolve_identity(
    *, cwd: str | None = None, job_dir: str | None = None,
    session: str | None = None, model: str | None = None,
) -> AgentIdentity:
    """Resolve an agent's identity from what it can tell the server about itself. The
    project comes from its cwd; the session + model are PROBED off its own transcript,
    never a self-report. Two probe paths: the CLAUDE_JOB_DIR anchor (precise), or — when
    that's absent (not every session sets it; decepticons surfaced this live, falling to
    the anonymous bucket) — the cwd's project dir, whose newest transcript is the active
    session. Only if BOTH fail does it fall to 'unknown'."""
    project = Path(cwd).name if cwd else None
    sid = session or _job_id(job_dir)
    # track WHICH channel gave us the model, so register_agent can grade it (see _MODEL_EC):
    # a passed-in model is the agent's own word; a probe off its transcript is an observation.
    model_method: str | None = "self_report" if model is not None else None
    if model is None and job_dir:
        model, _, _ = current_model(job_dir=job_dir)  # authoritative harness record
        if model is not None:
            model_method = "job_dir"
    if sid is None and cwd:  # no job dir → find the session by its project directory
        path = locate_transcript_by_cwd(cwd)
        if path is not None:
            sid = path.stem.split("-")[0]  # the 8-char handle, matching the job-id scheme
            if model is None:
                model = latest_model(_tail_lines(path))
                if model is not None:
                    model_method = "cwd"
    sid = sid or "unknown"
    return AgentIdentity(agent_id=f"agent:{sid}", session=sid, project=project,
                         model=model, cwd=cwd, model_method=model_method)


async def _link_once(
    actions: Actions, frm: uuid.UUID, to: uuid.UUID, ltype: str, src: str, when: datetime
) -> None:
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1", frm, to, ltype
    )
    if not exists:
        await actions.create_link(frm, to, ltype, src, when, _CONF, evidence_class=_EC)


# For the source_model property, the resolution METHOD is the provenance: reading the model off
# the agent's own transcript (job_dir) is a DIRECT_OBSERVATION of the harness record; the cwd
# fallback is a weaker DERIVED guess (it may read a co-located session's transcript); a
# self-reported model is the agent's own word. NB: self_report stays SELF_DECLARED for now —
# whether it should INVERT below observation (a swap is below the agent's own horizon) is a
# ruling still under discussion, deliberately NOT baked in here.
_MODEL_EC = {
    "job_dir": EvidenceClass.DIRECT_OBSERVATION,
    "cwd": EvidenceClass.DERIVED,
    "self_report": EvidenceClass.SELF_DECLARED,
}


async def register_agent(
    actions: Actions, identity: AgentIdentity, *, actor: str
) -> uuid.UUID:
    """Mint (idempotently) the Agent object + its org-chart links. The agent attributes
    its OWN registration (`source = agent:<session>`), SELF_DECLARED. Re-mount is a no-op
    (find-or-create + the kernel's byte-dup assertion skip absorb it)."""
    now = datetime.now(UTC)
    src = identity.agent_id
    a = await actions.create_or_find_object("Agent", identity.agent_id, src)
    label = f"{identity.model or 'claude'} in {identity.project or '?'}"
    await actions.assert_property(a, "name", label, src, now, _CONF, evidence_class=_EC)
    await actions.assert_property(a, "session", identity.session, src, now, _CONF,
                                  evidence_class=_EC)
    if identity.model:
        ec = _MODEL_EC.get(identity.model_method or "", EvidenceClass.SELF_DECLARED)
        await actions.assert_property(a, "source_model", identity.model, src, now,
                                      confidence_for(ec), evidence_class=ec.value)
    if identity.project:
        await actions.assert_property(a, "project", identity.project, src, now, _CONF,
                                      evidence_class=_EC)
        proj = await actions.create_or_find_object(
            "SoftwareProject", f"repo:{identity.project}", src)
        await actions.assert_property(proj, "name", identity.project, src, now, _CONF,
                                      evidence_class=_EC)
        await _link_once(actions, a, proj, "works_in", src, now)
    principal = await actions.create_or_find_object("Person", f"principal:{actor}", src)
    await actions.assert_property(principal, "name", actor, src, now, _CONF, evidence_class=_EC)
    await _link_once(actions, a, principal, "acts_for", src, now)
    return a
