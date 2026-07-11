"""Swarm lineage — the fractal fleet made first-class.

`resolve_identity`/`register_agent` (agents.py) capture an agent that MOUNTS. But an agent
spawns sub-agents (the Task/Agent tool), and those sub-agents rarely mount — they do a
bounded job and dissolve. Proven empirically (spawn experiment, decision ca66dc33): a
sub-agent that mounts COLLAPSES into its parent — it inherits the parent's $CLAUDE_JOB_DIR,
so the model-probe reads the PARENT's transcript and a Sonnet child registers as the Opus
parent. The swarm's work, and its DIFFERENT model, goes dark or mis-attributes upward.

But the harness records the whole tree on disk, and this module reconstructs it WITHOUT any
cooperation from the sub-agent:

    <project>/<session-uuid>/subagents/agent-<agentId>.jsonl       — the sub-agent transcript
    <project>/<session-uuid>/subagents/agent-<agentId>.meta.json   — { agentType, description,
                                                                        toolUseId, spawnDepth }

Two edges, kept DISTINCT (operator ruling): `spawned_by` is DELEGATION (child → its DIRECT
parent), `acts_for` is AUTHORITY (→ the root principal). The direct parent is deterministic:
a sub-agent's `toolUseId` is the spawn tool-call that created it, and that call lives in
exactly ONE transcript — its parent's. If no SIBLING sub-agent emitted it, the parent is the
root session (whose big transcript we then never have to parse). Model comes from each
sub-agent's OWN transcript (DIRECT_OBSERVATION) — the whole point: the graph finally records
that a Haiku, not the Opus parent, did the work.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.ingest.sessions import latest_model, models_in
from src.orchestrator.monitor import get_cursor, set_cursor
from src.orchestrator.swaps import classify_swap, swap_marker
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

# The swarm observer — reads the harness's on-disk record and writes the fleet tree under its
# OWN source (not the agent's self-attribution, not the text-miner's DERIVED), keeping the
# ownership boundary clean (rule #7): this observer only ever writes sub-agent Agent objects.
_SOURCE = "fleet-observer"
# Lineage + model are read straight from the harness record — a DIRECT_OBSERVATION (the same
# grade a job_dir transcript-probe earns in agents.py: better than a self-report, short of
# provider attestation). Structural facts (depth, type) come from the same record, same grade.
_EC = EvidenceClass.DIRECT_OBSERVATION
_CONF = confidence_for(_EC)


@dataclass
class SubAgent:
    """One node in the swarm tree, reconstructed from the harness's on-disk record."""

    agent_id: str          # "agent:<harness agentId>" — the provenance source string
    handle: str            # the raw harness agentId (a609c942…), the meta/transcript key
    session: str           # the ROOT session uuid (shared by the whole tree)
    project: str | None
    model: str | None      # read from THIS sub-agent's OWN transcript (not the parent's)
    model_history: tuple[str, ...]  # distinct models seen; >1 = a within-run swap on the child
    spawn_depth: int
    agent_type: str
    description: str
    tool_use_id: str       # the spawn tool-call that created this agent → resolves the parent
    transcript: Path
    last_active: datetime  # the transcript's mtime — the agent's last sign of life (lifecycle)
    backed_by_observation: bool  # Tier-1 act-detection: did it LOOK at all, or only HEAR children?


def _root_agent_id(session_uuid: str) -> str:
    """The mounted root's agent id — `agent:<first segment of the session uuid>`, the job-id
    scheme resolve_identity uses. The whole subagents/ tree hangs off this root."""
    return f"agent:{session_uuid.split('-')[0]}"


def _project_of(session_dir: Path) -> str | None:
    """The project name from the transcript dir: ~/.claude/projects/<-cwd-as-dashes>/<session>.
    The grandparent dir is the cwd with slashes→dashes; its last segment is the repo name."""
    parts = [p for p in session_dir.parent.name.split("-") if p]
    return parts[-1] if parts else None


def scan_subagents(session_dir: Path) -> list[SubAgent]:
    """Parse a session's `subagents/` tree into SubAgent nodes (pure file IO). Each node's
    model is read from its OWN transcript tail — the fix for the collapse. Sorted by
    (spawn_depth, handle) so a parent is always seen before its children."""
    subs_dir = session_dir / "subagents"
    if not subs_dir.is_dir():
        return []
    session_uuid = session_dir.name
    project = _project_of(session_dir)
    out: list[SubAgent] = []
    for meta_path in sorted(subs_dir.glob("agent-*.meta.json")):
        transcript = meta_path.with_name(meta_path.name.replace(".meta.json", ".jsonl"))
        if not transcript.is_file():
            continue
        try:
            meta = json.loads(meta_path.read_text())
        except (ValueError, OSError):
            continue
        handle = meta_path.name[len("agent-"):-len(".meta.json")]
        model: str | None = None
        history: list[str] = []
        try:  # read the whole (bounded) sub-agent transcript once: current model + swap history
            lines = transcript.read_text("utf-8", errors="replace").splitlines()
            history = models_in(lines)
            model = latest_model(lines)
        except OSError:
            pass
        last_active = datetime.fromtimestamp(transcript.stat().st_mtime, UTC)
        out.append(SubAgent(
            agent_id=f"agent:{handle}", handle=handle, session=session_uuid, project=project,
            model=model, model_history=tuple(history), spawn_depth=int(meta.get("spawnDepth", 1)),
            agent_type=str(meta.get("agentType", "")),
            description=str(meta.get("description", "")),
            tool_use_id=str(meta.get("toolUseId", "")), transcript=transcript,
            last_active=last_active, backed_by_observation=_has_own_observation(transcript),
        ))
    out.sort(key=lambda s: (s.spawn_depth, s.handle))
    return out


def _content_blocks(rec: dict[str, Any]) -> list[Any]:
    msg = rec.get("message")
    if isinstance(msg, dict):
        content = msg.get("content")
        if isinstance(content, list):
            return content
    return []


def _emitted_tool_use_ids(transcript: Path) -> set[str]:
    """Every tool_use id emitted in a transcript — the spawn calls it made live here. Resolves
    a child's `toolUseId` to the SIBLING that spawned it; a miss means the root did. Lines are
    pre-filtered cheaply so only the few tool_use records are JSON-parsed."""
    ids: set[str] = set()
    try:
        text = transcript.read_text("utf-8", errors="replace")
    except OSError:
        return ids
    for line in text.splitlines():
        if '"tool_use"' not in line or '"id"' not in line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        for block in _content_blocks(rec):
            if isinstance(block, dict) and block.get("type") == "tool_use" and block.get("id"):
                ids.add(str(block["id"]))
    return ids


# The tool names that carry a CHILD's result back to a parent (the "heard" conduit). Every other
# tool_use is the agent acting on the world ITSELF (a read, query, or edit).
_HEARD_CONDUITS = frozenset({"Agent", "Task"})


def _has_own_observation(transcript: Path) -> bool:
    """Tier-1 of the act-detection ladder (ruling 108ff2e8): did this agent perform ANY act of its
    own, or is everything it knows hearsay? An agent whose ONLY tool_uses are Agent/Task returns
    cannot have looked — it merely heard its children. Any other tool_use is the agent observing or
    acting itself. Structural and airtight; the finer 'did it observe THIS fact' (Tiers 2-3) is
    deferred, and this coarse floor is the conservative signal the credence rebuttal reads (we only
    clamp an ancestor that provably never looked, so a genuine verification is never deflated)."""
    try:
        text = transcript.read_text("utf-8", errors="replace")
    except OSError:
        return False
    for line in text.splitlines():
        if '"tool_use"' not in line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        for block in _content_blocks(rec):
            if (isinstance(block, dict) and block.get("type") == "tool_use"
                    and str(block.get("name", "")) not in _HEARD_CONDUITS):
                return True
    return False


def resolve_parents(subs: list[SubAgent]) -> dict[str, str]:
    """Map each sub-agent's agent_id → its DIRECT PARENT agent_id. A child's `toolUseId` is the
    spawn call that made it; whichever transcript EMITTED that id is the parent. Only SIBLING
    sub-agent transcripts are scanned (small); a miss means the root session spawned it (its
    toolUseId lives in the big root transcript, which we thus never have to parse)."""
    if not subs:
        return {}
    root = _root_agent_id(subs[0].session)
    emitter: dict[str, str] = {}
    for s in subs:
        for tid in _emitted_tool_use_ids(s.transcript):
            emitter[tid] = s.agent_id
    return {s.agent_id: emitter.get(s.tool_use_id, root) for s in subs}


async def _link_once(
    actions: Actions, frm: uuid.UUID, to: uuid.UUID, ltype: str, when: datetime
) -> bool:
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1", frm, to, ltype)
    if exists:
        return False
    await actions.create_link(frm, to, ltype, _SOURCE, when, _CONF, evidence_class=_EC.value)
    return True


async def _root_principal(actions: Actions, root_agent_id: str) -> str | None:
    """The root agent's principal (its acts_for target), so the swarm shares the root's
    authority. None if the root never mounted — authority then stays traversable via
    spawned_by → root → acts_for."""
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.canonical FROM objects o JOIN links l ON l.to_id=o.id "
        "JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical=$1 AND l.type='acts_for' AND o.type='Person' LIMIT 1", root_agent_id)


async def register_swarm(
    actions: Actions, session_dir: Path, *, principal: str | None = None
) -> dict[str, int]:
    """Reconstruct a session's swarm tree into the graph FROM DISK, with no reliance on the
    sub-agents mounting. Mints an Agent per sub-agent (its OWN model, DIRECT_OBSERVATION),
    wires `spawned_by` → its direct parent (delegation) and `acts_for` → the root principal
    (authority — distinct edges, per the ruling). Idempotent (find-or-create + byte-dup skip)."""
    subs = await asyncio.to_thread(scan_subagents, session_dir)
    if not subs:
        return {"agents": 0, "spawned_by": 0}
    parents = await asyncio.to_thread(resolve_parents, subs)
    now = datetime.now(UTC)
    principal = principal or await _root_principal(actions, _root_agent_id(subs[0].session))
    counts = {"agents": 0, "spawned_by": 0}
    for s in subs:
        a = await actions.create_or_find_object("Agent", s.agent_id, _SOURCE)
        label = f"{s.model or 'claude'} · {s.description or s.agent_type or 'sub-agent'}"[:120]

        async def prop(name: str, value: Any, obj: uuid.UUID = a) -> None:
            await actions.assert_property(obj, name, value, _SOURCE, now, _CONF,
                                          evidence_class=_EC.value)

        await prop("name", label)
        await prop("session", s.session)
        await prop("spawn_depth", s.spawn_depth)
        await prop("is_sidechain", True)
        await prop("last_active", s.last_active.isoformat())  # lifecycle: live vs historical
        await prop("backed_by_observation", s.backed_by_observation)  # credence rebuttal signal
        await prop("spawn_witnessed", True)  # scanned FROM its transcript — witnessed by definition
        if s.agent_type:
            await prop("agent_type", s.agent_type)
        if s.description:
            await prop("description", s.description)
        if s.model:
            await prop("source_model", s.model)
        if len(s.model_history) > 1:
            # a within-session swap on a SUB-agent (demoted mid-run) — the warm rug-pull the
            # mounted root gets flagged for, but a sub-agent never mounts, so this reconstruction
            # is its only confession. ONLY the transition matters: a swarm node has no operator
            # standing-choice to diverge from, so expected = its own current model (no divergence).
            v = classify_swap(s.model_history, s.model, expected=s.model or s.model_history[-1])
            await prop("model_swapped", swap_marker(v))  # DIRECT_OBSERVATION, like source_model
        if s.project:
            await prop("project", s.project)
            proj = await actions.create_or_find_object(
                "SoftwareProject", f"repo:{s.project}", _SOURCE)
            await _link_once(actions, a, proj, "works_in", now)
        # delegation: child → its DIRECT parent (a sibling sub-agent, or the root agent)
        parent = await actions.create_or_find_object("Agent", parents[s.agent_id], _SOURCE)
        if await _link_once(actions, a, parent, "spawned_by", now):
            counts["spawned_by"] += 1
        # authority (a DISTINCT edge): child → the root principal, when the root has mounted
        if principal:
            person = await actions.create_or_find_object("Person", principal, _SOURCE)
            await _link_once(actions, a, person, "acts_for", now)
        counts["agents"] += 1
    return counts


def normalize_spawn_id(raw: str | None) -> str | None:
    """The harness's subagent id → this module's keying. Hook payloads say `agent-a932dd…`,
    transcript filenames say `agent-a932dd….jsonl`, and scan_subagents keys the bare handle —
    normalize so a LIVE-registered spawn and the miner's later disk reconstruction are the
    SAME Agent object (find-or-create convergence, never a twin)."""
    rid = (raw or "").strip().removeprefix("agent-")
    return rid or None


async def register_spawn(
    actions: Actions, raw_id: str, *,
    agent_type: str | None = None, parent_agent: str | None = None,
    project: str | None = None, session: str | None = None,
    transcript: Path | None = None, done: bool = False,
    witnessed: bool | None = None,
) -> str | None:
    """Register ONE spawn the moment a hook sees it (the PreToolUse write-stamp, or
    SubagentStart/SubagentStop) — the LIVE half of register_swarm, so a spawn exists in the
    graph while it is still running instead of after the miner's next round. Same keying,
    same edges: `spawned_by` → the mounted parent as far as the live signal knows (the miner's
    full-tree pass later refines a sibling-spawned child's true parent — find-or-create means
    it converges on this object, never twins it), `acts_for` → the parent's principal.
    `transcript` (SubagentStop hands the child's own file) adds the OBSERVED model; `done`
    stamps last_active. Returns the child's agent id, or None on an unusable raw id.

    `witnessed` — did anything beyond the harness's ANNOUNCEMENT evidence this child? The
    ghost-spawn law (ruling 708a972d): this layer writes at DIRECT_OBSERVATION grade, so it
    must never testify above what it witnessed — Claude Code fires SubagentStart for
    ephemeral internal sidechains whose transcript never materializes, and registering one
    as a full child turned harness noise into an identity scare. Pass witnessed=True from
    paths where the child itself is acting (a hook-stamped tool call IS an observed act);
    a `transcript` argument folds in the disk truth (the file existing is a witness too,
    and an announcement whose named path never materialized stamps False — but an already-
    witnessed act is never un-witnessed by an unflushed file); leave None to stamp nothing.
    Distinct from `backed_by_observation` (the credence layer's look-vs-hearsay signal):
    unwitnessed means we never saw the child AT ALL, not that it only heard its children."""
    rid = normalize_spawn_id(raw_id)
    if rid is None:
        return None
    child = f"agent:{rid}"
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", child, _SOURCE)

    async def prop(name: str, value: Any) -> None:
        await actions.assert_property(a, name, value, _SOURCE, now, _CONF,
                                      evidence_class=_EC.value)

    await prop("is_sidechain", True)
    if agent_type:
        await prop("agent_type", agent_type)
        await prop("name", f"{agent_type} spawn"[:120])
    if session:
        await prop("session", session)
    if project:
        await prop("project", project)
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{project}", _SOURCE)
        await _link_once(actions, a, proj, "works_in", now)
    model: str | None = None
    if transcript is not None:
        def _read_model(path: Path = transcript) -> str | None:
            try:
                return latest_model(path.read_text("utf-8", errors="replace").splitlines())
            except OSError:
                return None
        model = await asyncio.to_thread(_read_model)
        if model:
            await prop("source_model", model)
        # the disk is a witness: a path the harness named but never materialized stamps the
        # spawn unwitnessed (ghost-spawn law, 708a972d) — unless an act was already seen
        witnessed = bool(witnessed) or await asyncio.to_thread(transcript.is_file)
    if witnessed is not None:
        await prop("spawn_witnessed", witnessed)
    if done:
        await prop("last_active", now.isoformat())
    if parent_agent:
        p = await actions.create_or_find_object("Agent", parent_agent, _SOURCE)
        await _link_once(actions, a, p, "spawned_by", now)
        principal = await _root_principal(actions, parent_agent)
        if principal:
            person = await actions.create_or_find_object("Person", principal, _SOURCE)
            await _link_once(actions, a, person, "acts_for", now)
    return child


async def sense_swarms(actions: Actions, root: Path) -> dict[str, int]:
    """Register EVERY session's swarm under `root` (~/.claude/projects). The miner's swarm
    pass — pure filesystem→graph, no LLM, idempotent. A session dir is any `<project>/<uuid>/`
    that has a `subagents/` child.

    MTIME WATERMARK (crunch residual, fleet-scale IO): an unchanged subagents/ tree is
    skipped without re-reading its transcripts — at 24 agents every 10 minutes the re-reads
    were real IO. The watermark is the tree's newest mtime, stored durably (the watermarks
    table, same infra the transcript cursors use); a touched tree re-registers (idempotent),
    a fresh worker after restart re-reads once and re-plants."""
    total = {"agents": 0, "spawned_by": 0, "skipped_unchanged": 0}
    session_dirs = await asyncio.to_thread(_session_dirs, root)
    for sdir in session_dirs:
        newest = await asyncio.to_thread(_tree_mtime, sdir / "subagents")
        key = f"swarm-mtime:{sdir}"
        seen = await get_cursor(actions.pool, key)
        if seen is not None and newest is not None and str(newest) == seen:
            total["skipped_unchanged"] += 1
            continue
        for k, v in (await register_swarm(actions, sdir)).items():
            total[k] = total.get(k, 0) + v
        if newest is not None:
            await set_cursor(actions.pool, key, str(newest))
    return total


def _tree_mtime(subs_dir: Path) -> float | None:
    """The newest mtime under a subagents/ tree (pure IO) — the change signal the watermark
    stores. None when the tree is missing/unreadable (then we never skip)."""
    try:
        times = [p.stat().st_mtime for p in subs_dir.glob("agent-*")]
        return max(times) if times else None
    except OSError:
        return None


def _session_dirs(root: Path) -> list[Path]:
    """Every `<project>/<session-uuid>/` dir that has a `subagents/` child (pure IO)."""
    return [p.parent for p in root.expanduser().glob("*/*/subagents") if p.is_dir()]
