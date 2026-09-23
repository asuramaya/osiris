"""Swarm lineage: the fractal fleet made first-class.

`resolve_identity`/`register_agent` (agents.py) capture an agent that mounts. But an agent
spawns sub-agents (the Task/Agent tool), and those sub-agents rarely mount: they do a
bounded job and dissolve. Proven empirically by a spawn experiment: a sub-agent that mounts
collapses into its parent. It inherits the parent's $CLAUDE_JOB_DIR, so the model-probe reads
the parent's transcript and a Sonnet child registers as the Opus parent. The swarm's work,
and its different model, goes dark or mis-attributes upward.

But the harness records the whole tree on disk, and this module reconstructs it without any
cooperation from the sub-agent:

    <project>/<session-uuid>/subagents/agent-<agentId>.jsonl       (the sub-agent transcript)
    <project>/<session-uuid>/subagents/agent-<agentId>.meta.json   (agentType, description,
                                                                     toolUseId, spawnDepth)

Two edges are kept distinct: `spawned_by` is delegation (child to its direct parent),
`acts_for` is authority (to the root principal). The direct parent is deterministic: a
sub-agent's `toolUseId` is the spawn tool call that created it, and that call lives in
exactly one transcript, its parent's. If no sibling sub-agent emitted it, the parent is the
root session (whose large transcript we then never have to parse). Model comes from each
sub-agent's own transcript (direct observation), which is the point: the graph finally
records that a smaller model, not the larger parent model, did the work.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from collections import defaultdict
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

# The swarm observer reads the harness's on-disk record and writes the fleet tree under its
# own source, not the agent's self-attribution and not the text-miner's derived source,
# keeping the ownership boundary clean: this observer only ever writes sub-agent Agent objects.
_SOURCE = "fleet-observer"
_log = logging.getLogger("osiris.lineage")
# Lineage and model are read straight from the harness record: a direct observation (the same
# grade a job_dir transcript probe earns in agents.py, better than a self-report, short of
# provider attestation). Structural facts (depth, type) come from the same record, same grade.
_EC = EvidenceClass.DIRECT_OBSERVATION
_CONF = confidence_for(_EC)


@dataclass
class SubAgent:
    """One node in the swarm tree, reconstructed from the harness's on-disk record."""

    agent_id: str          # "agent:<harness agentId>", the provenance source string
    handle: str            # the raw harness agentId (a609c942...), the meta/transcript key
    session: str           # the root session uuid, shared by the whole tree
    project: str | None
    model: str | None      # read from this sub-agent's own transcript, not the parent's
    model_history: tuple[str, ...]  # distinct models seen; >1 means a within-run swap
    spawn_depth: int
    agent_type: str
    description: str
    tool_use_id: str       # the spawn tool call that created this agent; resolves the parent
    transcript: Path
    last_active: datetime  # the transcript's mtime: the agent's last sign of life
    backed_by_observation: bool  # act-detection tier 1: did it look at all, or only hear children?


def _root_agent_id(session_uuid: str) -> str:
    """The mounted root's agent id: `agent:<first segment of the session uuid>`, the job-id
    scheme resolve_identity uses. The whole subagents/ tree hangs off this root."""
    return f"agent:{session_uuid.split('-')[0]}"


def _project_of(session_dir: Path) -> str | None:
    """The project name from the transcript dir: ~/.claude/projects/<-cwd-as-dashes>/<session>."""
    from src.ingest.harness.claude_jsonl import decode_claude_project_name
    return decode_claude_project_name(session_dir.parent.name) or None


async def scan_subagents(session_dir: Path) -> list[SubAgent]:
    """Parse a session's `subagents/` tree into SubAgent nodes. Each node's model is read
    from its own transcript tail, which is the fix for the collapse described above. Sorted
    by (spawn_depth, handle) so a parent is always seen before its children. Every actual file
    read runs through `asyncio.to_thread` (a bare, uncalled attribute passed to `to_thread`,
    never an inline `.read_text()` call, matching the convention the blocking-transcript-read
    guard expects), even though these are bounded sub-agent transcripts, never the 200-470MB
    main-session kind."""
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
            meta_text = await asyncio.to_thread(meta_path.read_text)
            meta = json.loads(meta_text)
        except (ValueError, OSError):
            continue
        handle = meta_path.name[len("agent-"):-len(".meta.json")]
        model: str | None = None
        history: list[str] = []
        try:  # read the whole (bounded) sub-agent transcript once: current model + swap history
            text = await asyncio.to_thread(transcript.read_text, "utf-8", "replace")
            lines = text.splitlines()
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
            last_active=last_active,
            backed_by_observation=await _has_own_observation(transcript),
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


async def _emitted_tool_use_ids(transcript: Path) -> set[str]:
    """Every tool_use id emitted in a transcript: the spawn calls it made live here. Resolves
    a child's `toolUseId` to the sibling that spawned it; a miss means the root did. Lines are
    pre-filtered cheaply so only the few tool_use records are JSON-parsed."""
    ids: set[str] = set()
    try:
        text = await asyncio.to_thread(transcript.read_text, "utf-8", "replace")
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


# The tool names that carry a child's result back to a parent (the "heard" conduit). Every other
# tool_use is the agent acting on the world itself (a read, query, or edit).
_HEARD_CONDUITS = frozenset({"Agent", "Task"})


async def _has_own_observation(transcript: Path) -> bool:
    """Tier 1 of the act-detection scale: did this agent perform any act of its own, or is
    everything it knows hearsay? An agent whose only tool_uses are Agent/Task returns cannot
    have looked; it merely heard its children. Any other tool_use is the agent observing or
    acting itself. Structural and airtight; the finer "did it observe this fact" distinction
    (tiers 2-3) is deferred, and this coarse floor is the conservative signal the credence
    rebuttal reads (it only clamps an ancestor that provably never looked, so a genuine
    verification is never deflated)."""
    try:
        text = await asyncio.to_thread(transcript.read_text, "utf-8", "replace")
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


async def resolve_parents(subs: list[SubAgent]) -> dict[str, str]:
    """Map each sub-agent's agent_id to its direct parent agent_id. A child's `toolUseId` is the
    spawn call that made it; whichever transcript emitted that id is the parent. Only sibling
    sub-agent transcripts are scanned (small); a miss means the root session spawned it (its
    toolUseId lives in the large root transcript, which we thus never have to parse)."""
    if not subs:
        return {}
    root = _root_agent_id(subs[0].session)
    emitter: dict[str, str] = {}
    for s in subs:
        for tid in await _emitted_tool_use_ids(s.transcript):
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
    authority. None if the root never mounted; authority then stays traversable via
    spawned_by to root to acts_for."""
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT o.canonical FROM objects o JOIN links l ON l.to_id=o.id "
        "JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical=$1 AND l.type='acts_for' AND o.type='Person' LIMIT 1", root_agent_id)


async def register_swarm(
    actions: Actions, session_dir: Path, *, principal: str | None = None
) -> dict[str, int]:
    """Reconstruct a session's swarm tree into the graph from disk, with no reliance on the
    sub-agents mounting. Mints an Agent per sub-agent (its own model, direct observation
    grade), wires `spawned_by` to its direct parent (delegation) and `acts_for` to the root
    principal (authority, a distinct edge). Idempotent (find-or-create plus byte-dup skip)."""
    subs = await scan_subagents(session_dir)
    if not subs:
        return {"agents": 0, "spawned_by": 0}
    parents = await resolve_parents(subs)
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
        await prop("backed_by_observation", s.backed_by_observation)  # credence signal
        await prop("spawn_witnessed", True)  # scanned from its transcript: witnessed by definition
        if s.agent_type:
            await prop("agent_type", s.agent_type)
        if s.description:
            await prop("description", s.description)
        if s.model:
            await prop("source_model", s.model)
        if len(s.model_history) > 1:
            # A within-session swap on a sub-agent (demoted mid-run): the same kind of model
            # substitution the mounted root gets flagged for, but a sub-agent never mounts, so
            # this reconstruction is its only record of it. Only the transition matters: a
            # swarm node has no standing choice to diverge from, so expected = its own current
            # model (no divergence).
            v = classify_swap(s.model_history, s.model, expected=s.model or s.model_history[-1])
            await prop("model_swapped", swap_marker(v))  # direct observation, like source_model
        if s.project:
            await prop("project", s.project)
            # Reuses capture.py's single validation choke point verbatim rather than
            # re-validating here: this same gap was found alongside register_spawn but left
            # unaddressed for a time, and was later closed by matching register_spawn's own
            # pattern exactly rather than reinventing it. A malformed s.project (a raw cwd, a
            # placeholder) still stamps the agent's own project property above, a claim about
            # the caller that is harmless as text, but mints no phantom SoftwareProject.
            from src.orchestrator.capture import _validate_repo_name
            try:
                _validate_repo_name(s.project, s.project)
            except ValueError as exc:
                # Log this rather than fail silently: a skip that says nothing is
                # indistinguishable from a clean pass. register_swarm returns only aggregate
                # counts, so there is no result field to carry this on; a warning is the
                # honest surface until one exists.
                _log.warning("register_swarm(%s): refusing to mint a SoftwareProject from "
                            "project=%r — %s", s.agent_id, s.project, exc)
            else:
                # Resolves by name before minting: a bare create_or_find_object on the
                # canonical alone would mint a duplicate the instant a project's own name has
                # moved since this disk basename was recorded, since canonical never
                # re-points. Uses the same _resolve_repo choke point create_project/
                # _mint_or_find_repo already trust.
                from src.orchestrator.capture import _resolve_repo
                proj = await _resolve_repo(actions.pool, s.project)
                if proj is None:
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
    """The harness's subagent id, normalized to this module's keying. Hook payloads say
    `agent-a932dd...`, transcript filenames say `agent-a932dd....jsonl`, and scan_subagents
    keys the bare handle. Normalizing keeps a live-registered spawn and the miner's later
    disk reconstruction pointed at the same Agent object (find-or-create convergence, never
    a duplicate)."""
    rid = (raw or "").strip().removeprefix("agent-")
    return rid or None


async def register_spawn(
    actions: Actions, raw_id: str, *,
    agent_type: str | None = None, parent_agent: str | None = None,
    project: str | None = None, session: str | None = None,
    transcript: Path | None = None, done: bool = False,
    witnessed: bool | None = None,
) -> str | None:
    """Register one spawn the moment a hook sees it (the PreToolUse write-stamp, or
    SubagentStart/SubagentStop): the live half of register_swarm, so a spawn exists in the
    graph while it is still running instead of after the miner's next round. Same keying,
    same edges: `spawned_by` points to the mounted parent as far as the live signal knows
    (the miner's full-tree pass later refines a sibling-spawned child's true parent;
    find-or-create means it converges on this object, never duplicates it), `acts_for` points
    to the parent's principal. `transcript` (SubagentStop hands the child's own file) adds the
    observed model; `done` stamps last_active. Returns the child's agent id, or None on an
    unusable raw id.

    `witnessed`: did anything beyond the harness's announcement evidence this child? This
    layer writes at direct-observation grade, so it must never testify above what it
    witnessed. Claude Code fires SubagentStart for ephemeral internal sidechains whose
    transcript never materializes, and registering one as a full child turned harness noise
    into a false identity alarm. Pass witnessed=True from paths where the child itself is
    acting (a hook-stamped tool call is an observed act); a `transcript` argument folds in the
    disk truth (the file existing is a witness too, and an announcement whose named path never
    materialized stamps False, but an already-witnessed act is never un-witnessed by an
    unflushed file); leave None to stamp nothing. Distinct from `backed_by_observation` (the
    credence layer's look-vs-hearsay signal): unwitnessed means we never saw the child at all,
    not that it only heard its children."""
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
        # Reuses capture.py's single validation choke point rather than re-validating here:
        # this was the only SoftwareProject-mint site in the codebase without it, safe only
        # because every live caller already passes a clean label, never enforced at the mint
        # itself. A malformed `project` (a raw cwd, a placeholder) still stamps the agent's
        # own `project` property above, a claim about the caller that is harmless as text,
        # but mints no phantom SoftwareProject.
        from src.orchestrator.capture import _validate_repo_name
        try:
            _validate_repo_name(project, project)
        except ValueError as exc:
            # Log this rather than fail silently: a skip that says nothing is
            # indistinguishable from a clean pass. register_spawn returns only the child's
            # id, so there is no result field to carry this on; a warning is the honest
            # surface until one exists.
            _log.warning("register_spawn(%s): refusing to mint a SoftwareProject from "
                        "project=%r — %s", child, project, exc)
        else:
            # Resolves by name before minting, the same fix as register_swarm's own sibling
            # site above, via the same _resolve_repo choke point.
            from src.orchestrator.capture import _resolve_repo
            proj = await _resolve_repo(actions.pool, project)
            if proj is None:
                proj = await actions.create_or_find_object(
                    "SoftwareProject", f"repo:{project}", _SOURCE)
            await _link_once(actions, a, proj, "works_in", now)
    model: str | None = None
    if transcript is not None:
        try:
            text = await asyncio.to_thread(transcript.read_text, "utf-8", "replace")
            model = latest_model(text.splitlines())
        except OSError:
            model = None
        if model:
            await prop("source_model", model)
        # The disk is a witness: a path the harness named but never materialized stamps the
        # spawn unwitnessed, unless an act was already seen.
        witnessed = bool(witnessed) or await asyncio.to_thread(transcript.is_file)
    if witnessed is not None:
        await prop("spawn_witnessed", witnessed)
    if done and bool(witnessed):
        # A liveness stamp must be earned by an act, never granted by an announcement alone:
        # a stop-announcement for a child that nothing ever witnessed, no transcript
        # materialized, no act observed, must not stamp life. 42 of the 44 spawns registered
        # on 2026-07-14 were such ghosts (the compaction summarizer, one per step), and the
        # stamp made each render live in the fleet tree for 15 minutes.
        await prop("last_active", now.isoformat())
    if parent_agent:
        p = await actions.create_or_find_object("Agent", parent_agent, _SOURCE)
        await _link_once(actions, a, p, "spawned_by", now)
        principal = await _root_principal(actions, parent_agent)
        if principal:
            person = await actions.create_or_find_object("Person", principal, _SOURCE)
            await _link_once(actions, a, person, "acts_for", now)
        # The patronym: a sub-agent takes its parent's own displayed name plus a birth
        # ordinal, e.g. 'Example XL.1', 'Example XIII.4', so the name carries the provenance
        # and a lost link can orphan nobody. A label, never a handle: minted as an assertion
        # outside the claim namespace, once (repeated register_spawn calls converge on the
        # same child; the ordinal must not drift). An anonymous parent mints nothing; a later
        # backfill names those children the day their parent registers or is identified.
        try:
            has = await actions.pool.fetchval(
                "SELECT 1 FROM current_assertions ca WHERE ca.object_id=$1 "
                "AND ca.name='patronym'", a)
            if not has:
                pat = await patronym_for(actions, parent_agent)
                if pat:
                    await prop("patronym", pat)
                    await prop("name", f"{pat} · {agent_type}" if agent_type else pat)
        except Exception:  # noqa: BLE001 - a name is a bonus; the spawn record never dies of one
            _log.debug("patronym mint failed for %s", child, exc_info=True)
    return child


async def patronym_for(actions: Actions, parent_agent: str) -> str | None:
    """'<parent's displayed name>.<birth ordinal>' for that parent's next child. The roman
    numeral belongs to the parent; children ride it dotted. None when the parent's lineage
    holds no claimed handle. The ordinal is the count of the parent's spawned_by edges (this
    child's own edge included, so it is this child's number); two spawns registering in the
    same instant can in principle draw the same ordinal, a display collision the lint can
    renumber, never an identity fact, so no lock is worth the contention here."""
    from src.orchestrator.agents import seat_label

    pool = actions.pool
    handle = await pool.fetchval(
        "SELECT a.value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='handle' AND (o.canonical=$1 OR $1 LIKE o.canonical||'-%') "
        "ORDER BY a.observed_at DESC LIMIT 1", parent_agent)
    if not handle:
        return None
    gen = await pool.fetchval(
        "SELECT a.value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='seat_generation' AND o.canonical=$1 LIMIT 1", parent_agent)
    label = seat_label(parent_agent, str(handle),
                       int(gen) if gen and str(gen).isdigit() else None) or str(handle)
    n = await pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.type='spawned_by' AND p.canonical=$1", parent_agent)
    return f"{label}.{max(int(n or 0), 1)}"


# Subagent filing: a sub-agent is never a first-class fleet member; it files under its
# spawner, permanently. Surveyed before building: of 2,679 active 17-hex subagent Agent
# objects fleet-wide, 2,672 (99%) already carry a spawned_by edge (register_swarm's and
# register_spawn's own work), 2,406 of those already carry a patronym (register_spawn's live
# path). The real gap is the ~266 backfill-only names and the 7 edge-less stragglers whose
# `session` property is their only pointer home, plus the status-follows-parent flip, which
# exists nowhere yet.
_SUBAGENT_PATTERN = "^agent:a[0-9a-f]{16}$"
_LIVE_SECS = 900  # the fleet's one liveness window, shared with seats.py, liveness.py, the roster


async def _resolve_subagent_parent(
    actions: Actions, subagent_oid: uuid.UUID,
) -> tuple[str | None, bool]:
    """The subagent's direct parent: its spawned_by edge where one exists, else its
    `session` property's root agent id (resolve_parents' own fallback, "a miss means the
    root session spawned it," reapplied at filing time for the small slice that predates even
    that reconstruction). `(None, False)` only when neither exists. Pure read; writes
    nothing, safe to call during a dry-run classification pass.

    Returns `(parent, verified)`: a spawned_by edge is always real (`verified=True`), but the
    session-property fallback synthesizes an id from a raw string that may never have been
    registered as an Agent object anywhere, the exact gap that let this function disagree with
    `identify_agent`/`doors()` (which does real existence resolution and reports zero matches
    for the same unregistered id). This still returns the synthesized id even when unverified:
    `file_subagent`'s own mint-on-demand contract for the fleet-wide-straggler case (a
    session-only child, no pre-existing parent object) depends on getting a non-None string
    back to create_or_find_object, not a refusal. Only `verified` tells a caller that wants
    identify_agent-grade honesty (file_subagents' own dry-run classification pass) whether
    this id is a confirmed object or still just a plausible guess."""
    parent = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='spawned_by' LIMIT 1", subagent_oid)
    if parent:
        return str(parent), True
    session = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='session' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
        subagent_oid)
    if not session:
        return None, False
    candidate = f"agent:{session}"
    verified = bool(await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Agent' AND canonical=$1", candidate))
    return candidate, verified


async def _parent_live(actions: Actions, parent: str) -> bool:
    """Is the exact parent generation (not its whole lineage) live right now? A sub-agent was
    spawned by one specific turn; if a newer generation has since succeeded it, that turn is
    over and the sub-agent it spawned can never resume, even though the lineage continues.

    Cache-based and deliberately not cross-checked: uses `agent_mounts.last_seen` freshness
    only, never verified against a harder liveness source. Unlike a real refusal gate, this
    only decides a status label, whether `file_subagent` flips a spawned sub-agent to
    'historical', and getting it wrong in either direction is a mislabeled record, never a
    lost result or a forked identity. Called at every subagent filing, not a rare admin
    operation, so the cheap read stays the right trade here; a caller that needs a harder
    guarantee should ask the real authority itself, not assume this one already did."""
    return bool(await actions.pool.fetchval(
        "SELECT max(last_seen) > now() - make_interval(secs => $2) FROM agent_mounts "
        "WHERE agent_id=$1", parent, float(_LIVE_SECS)))


async def file_subagent(
    actions: Actions, *, subagent_id: str, actor: str, patronym_ordinal: int | None = None,
) -> dict[str, Any]:
    """Files one subagent: (1) attributes it to its spawner, refusing loudly when neither a
    spawned_by edge nor a `session` property resolves one (a survey found zero such cases
    fleet-wide, but the refusal stands rather than guess). (2) stamps the X.n patronym name
    (patronym_for's own label-plus-ordinal shape) when it doesn't already carry one:
    idempotent, never renames an already-named sub-agent. (3) flips status to 'historical' via
    Actions.set_status (a real object_event, never raw SQL) when the exact parent generation
    is no longer live; a sub-agent whose parent is live is filed (attributed and named) but
    never status-flipped, so an agent's own research sub-agents mid-work are never buried.

    `patronym_ordinal`, when given, overrides patronym_for's own count-based ordinal. That
    count is every spawned_by edge into the parent, named or not: correct for the live path
    (one child registers at a time, so the count is this child's rank the instant it's read)
    but wrong for a backfill where every sibling's edge already exists, since every unnamed
    sibling would compute the same total and collide on one name. file_subagents (the sweep)
    computes real per-parent ordinals once and passes them in; a standalone call is safe
    without one only when no other unnamed sibling of the same parent is being filed at the
    same time."""
    row = await actions.pool.fetchrow(
        "SELECT id, status FROM objects WHERE canonical=$1 AND type='Agent'", subagent_id)
    if row is None:
        return {"error": f"no such subagent: {subagent_id!r}"}
    oid = row["id"]
    now = datetime.now(UTC)
    parent, _parent_verified = await _resolve_subagent_parent(actions, oid)
    if not parent:
        return {"error": f"{subagent_id} has neither a spawned_by edge nor a session "
                         "property — cannot attribute to a spawner"}
    parent_oid = await actions.create_or_find_object("Agent", parent, actor)
    linked = await _link_once(actions, oid, parent_oid, "spawned_by", now)

    named: str | None = None
    already_named = bool(await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions WHERE object_id=$1 AND name='patronym'", oid))
    if not already_named:
        if patronym_ordinal is not None:
            from src.orchestrator.agents import seat_label
            handle = await actions.pool.fetchval(
                "SELECT a.value#>>'{}' FROM current_assertions a "
                "JOIN objects o ON o.id=a.object_id "
                "WHERE a.name='handle' AND (o.canonical=$1 OR $1 LIKE o.canonical||'-%') "
                "ORDER BY a.observed_at DESC LIMIT 1", parent)
            if handle:
                gen = await actions.pool.fetchval(
                    "SELECT a.value#>>'{}' FROM current_assertions a "
                    "JOIN objects o ON o.id=a.object_id "
                    "WHERE a.name='seat_generation' AND o.canonical=$1 LIMIT 1", parent)
                label = seat_label(parent, str(handle),
                                   int(gen) if gen and str(gen).isdigit() else None) or str(handle)
                named = f"{label}.{patronym_ordinal}"
        else:
            named = await patronym_for(actions, parent)
        if named:
            async def prop(name: str, value: Any) -> None:
                await actions.assert_property(oid, name, value, actor, now, _CONF,
                                              evidence_class=_EC.value)
            await prop("patronym", named)
            await prop("name", named)

    live = await _parent_live(actions, parent)
    flipped = False
    if not live and row["status"] == "active":
        await actions.set_status(
            oid, "historical",
            f"ephemeral subagent, parent {parent} not live — status follows the spawner "
            "(ruling 0f76458c)", actor)
        flipped = True
    return {"subagent": subagent_id, "parent": parent, "spawned_by_linked": linked,
            "named": named, "already_named": already_named, "parent_live": live,
            "status_flipped_historical": flipped}


_PATRONYM_ORDINAL = re.compile(r"\.(\d+)$")


async def file_subagents(
    actions: Actions, *, project: str | None = None, dry_run: bool = True, actor: str,
    limit: int = 4000,
) -> dict[str, Any]:
    """The sweep: runs file_subagent's resolver over every active 17-hex subagent Agent
    object in scope (`project=` narrows it; None is fleet-wide). Dry-run (the default) writes
    nothing and reports per-class counts, attributable_parent_dead / attributable_parent_live
    / unattributable, plus a bounded sample, so a caller sees a scope's shape before
    committing to it. The intended rollout sequence is a small test scope first, results
    reviewed before going live, then a fleet-wide dry-run: never the reverse.

    Ordinals are computed here, once, per parent: the reason this sweep exists rather than a
    loop over file_subagent. A backfill's siblings mostly already have their spawned_by edge,
    so patronym_for's own count-based ordinal would hand every unnamed sibling of one parent
    the same number. This groups unnamed candidates by resolved parent, finds each parent's
    highest already-used ordinal (parsed off existing patronym suffixes, fleet-wide, not
    just this scope, so a project-scoped sweep never collides with a name minted elsewhere),
    and hands out the next integers in a stable order (oldest `last_active` first)."""
    rows = await actions.pool.fetch(
        "SELECT o.id, o.canonical, o.status, "
        " (SELECT a.value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='last_active' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS last_active, "
        " (SELECT a.value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='patronym' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS patronym "
        "FROM objects o WHERE o.type='Agent' AND o.status='active' "
        f"AND o.canonical ~ '{_SUBAGENT_PATTERN}' "
        "AND ($1::text IS NULL OR EXISTS (SELECT 1 FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='project' AND a.value#>>'{}' = $1)) "
        "ORDER BY o.canonical LIMIT $2", project, limit)

    candidates = []
    for r in rows:
        parent, parent_verified = await _resolve_subagent_parent(actions, r["id"])
        candidates.append({"oid": r["id"], "canonical": r["canonical"],
                           "last_active": r["last_active"] or "", "patronym": r["patronym"],
                           "parent": parent, "parent_verified": parent_verified})
    unattributable = [c for c in candidates if not c["parent"]]
    attributable = [c for c in candidates if c["parent"]]

    live_cache: dict[str, bool] = {}
    for c in attributable:
        p = c["parent"]
        if p not in live_cache:
            live_cache[p] = await _parent_live(actions, p)
        c["parent_live"] = live_cache[p]
    parent_dead = [c for c in attributable if not c["parent_live"]]
    parent_live_list = [c for c in attributable if c["parent_live"]]

    # per-parent ordinal assignment for whoever still needs a name
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for c in attributable:
        if not c["patronym"]:
            by_parent[c["parent"]].append(c)
    ordinal_plan: dict[uuid.UUID, int] = {}
    for parent, kids in by_parent.items():
        used = await actions.pool.fetch(
            "SELECT a.value#>>'{}' AS patronym FROM current_assertions a "
            "JOIN links l ON l.from_id=a.object_id "
            "JOIN objects p ON p.id=l.to_id AND p.canonical=$1 "
            "WHERE a.name='patronym' AND l.type='spawned_by'", parent)
        highest = 0
        for u in used:
            m = _PATRONYM_ORDINAL.search(u["patronym"] or "")
            if m:
                highest = max(highest, int(m.group(1)))
        kids.sort(key=lambda c: c["last_active"])
        for i, k in enumerate(kids, start=1):
            ordinal_plan[k["oid"]] = highest + i

    counts = {"attributable_parent_dead": len(parent_dead),
             "attributable_parent_live": len(parent_live_list),
             "unattributable": len(unattributable),
             "attributable_parent_unverified": sum(
                 1 for c in attributable if not c["parent_verified"])}
    sample = [{"subagent": c["canonical"], "parent": c["parent"],
               "parent_verified": c["parent_verified"],
               "will_name": ordinal_plan.get(c["oid"]) is not None,
               "will_flip_historical": not c["parent_live"]}
              for c in attributable[:20]]

    if dry_run:
        return {"scope": project or "fleet", "candidates": len(candidates), "counts": counts,
                "sample": sample, "unattributable_ids": [c["canonical"] for c in unattributable],
                "note": "DRY-RUN — nothing written; pass dry_run=False to file"}

    filed = [await file_subagent(actions, subagent_id=c["canonical"], actor=actor,
                                 patronym_ordinal=ordinal_plan.get(c["oid"]))
             for c in attributable]
    return {"scope": project or "fleet", "candidates": len(candidates), "counts": counts,
            "filed": len(filed), "unattributable_ids": [c["canonical"] for c in unattributable]}


async def sense_swarms(actions: Actions, root: Path) -> dict[str, int]:
    """Register every session's swarm under `root` (~/.claude/projects). The miner's swarm
    pass: pure filesystem-to-graph, no LLM, idempotent. A session dir is any `<project>/<uuid>/`
    that has a `subagents/` child.

    Mtime watermark, for fleet-scale IO: an unchanged subagents/ tree is skipped without
    re-reading its transcripts, since at 24 agents every 10 minutes the re-reads were real IO.
    The watermark is the tree's newest mtime, stored durably (the watermarks table, the same
    infrastructure the transcript cursors use); a touched tree re-registers (idempotent), and
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
    """The newest mtime under a subagents/ tree (pure IO): the change signal the watermark
    stores. None when the tree is missing or unreadable (then we never skip)."""
    try:
        times = [p.stat().st_mtime for p in subs_dir.glob("agent-*")]
        return max(times) if times else None
    except OSError:
        return None


def _session_dirs(root: Path) -> list[Path]:
    """Every `<project>/<session-uuid>/` dir that has a `subagents/` child (pure IO)."""
    return [p.parent for p in root.expanduser().glob("*/*/subagents") if p.is_dir()]


def _transcript_exists_for_handle(root: Path, handle: str) -> bool:
    """Does any session anywhere under `root` carry a real `agent-<handle>.meta.json` for
    this subagent: the on-disk corroboration `resolve_parents` uses to attribute a
    disk-reconstructed spawn, and which the live path (register_spawn, called by
    mcp_server's `_actor_for` the instant a hook stamps a call with a subagent_id) never
    checks at all before minting a permanent `spawned_by` fact. Pure IO, bounded by one
    glob."""
    try:
        return next(root.expanduser().glob(f"*/*/subagents/agent-{handle}.meta.json"), None) \
            is not None
    except OSError:
        return False


async def unwitnessed_spawns(
    actions: Actions, agent_id: str, *, root: Path,
) -> list[dict[str, Any]]:
    """The read a prior fix requested: an agent, or the operator, should be able to
    enumerate what is executing under their identity right now that they did not spawn.
    Every live `spawned_by` child of `agent_id` for which no transcript or meta file has ever
    materialized anywhere under `root`: the exact shape of an earlier specimen (three
    subagents parented to one agent, zero corresponding files on disk anywhere on the host).

    The root-cause gap this surfaces, not yet fixed: `_actor_for` (mcp_server.py) calls
    register_spawn with `witnessed=True` unconditionally the moment a hook stamps a call
    with a subagent_id, on the premise that a hook-stamped tool call is an observed act, and
    register_spawn mints the `spawned_by` edge as a permanent fact on that signal alone, with
    no tool_use_id captured (unlike the disk-reconstruction path's `resolve_parents`, which
    only ever attributes a parent it can find emitting that exact spawn call in a real
    transcript). A connection whose cached identity is wrong, for whatever reason, harness-side
    or otherwise, mints an unfalsifiable fact with total confidence and no way to later
    reconcile it against the parent's own transcript, because the one thing that would let a
    later pass verify it (the tool_use_id) was never recorded. This read is the audit that
    exists because that gap exists, not a fix for it: the fix (recording a verifiable signal
    at spawn time, or deferring the edge until a transcript actually witnesses the child) is
    scoped, not built, pending confirmation from live evidence this read alone cannot supply
    (which of two mechanisms handed the wrong connection its identity is a question this
    function does not answer)."""
    rows = await actions.pool.fetch(
        "SELECT c.canonical, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=c.id "
        "   AND a.name='session' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS session, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=c.id "
        "   AND a.name='project' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS project, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=c.id "
        "   AND a.name='last_active' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS last_active, "
        " l.first_seen AS spawned_by_since "
        "FROM links l JOIN objects p ON p.id=l.to_id JOIN objects c ON c.id=l.from_id "
        "WHERE p.canonical=$1 AND l.type='spawned_by' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent_id)
    handles = [(r["canonical"], r["canonical"].removeprefix("agent:")) for r in rows]
    exists = await asyncio.gather(
        *(asyncio.to_thread(_transcript_exists_for_handle, root, h) for _, h in handles))
    out = []
    for r, (canonical, _h), found in zip(rows, handles, exists, strict=True):
        if found:
            continue
        out.append({
            "child": canonical, "session": r["session"], "project": r["project"],
            "last_active": r["last_active"],
            "spawned_by_since": r["spawned_by_since"].isoformat(),
            "transcript_found_on_disk": False,
        })
    return out
