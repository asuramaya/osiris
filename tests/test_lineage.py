"""Swarm lineage — the fractal fleet reconstructed from the harness's on-disk record.

Proven empirically (spawn experiment): a sub-agent inherits the parent's job_dir, so mounting
COLLAPSES it into the parent (a Sonnet child records as the Opus parent). These tests drive the
reconstruction that fixes it — from `subagents/agent-<id>.{jsonl,meta.json}`, with fixtures that
mirror the REAL layout: model in `{"type":"assistant","message":{"model":…}}`, a spawn as a
`tool_use` block whose id is the child's `toolUseId`.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.lineage import (
    register_swarm,
    resolve_parents,
    scan_subagents,
    sense_swarms,
)
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 6, tzinfo=UTC)
_ROOT = "agent:abc12345"  # first segment of the session uuid = the mounted root's id


def _assistant(model: str, *tool_use_ids: str) -> str:
    """One assistant transcript line: carries the model, and emits the given tool_use ids
    (the spawn calls this agent made — how a parent is linked to the children it spawned)."""
    content = [{"type": "tool_use", "id": t, "name": "Agent", "input": {}} for t in tool_use_ids]
    content.append({"type": "text", "text": "ok"})
    return json.dumps({"type": "assistant", "message": {"model": model, "content": content}})


def _write_swarm(tmp_path: Path) -> Path:
    """A root session with a depth-1 child (Sonnet) that spawned a depth-2 grandchild (Haiku).
    The child's transcript EMITS the grandchild's spawn tool_use (`tu-gc`); the child's own
    `toolUseId` (`tu-child`) is emitted by no sibling → its parent resolves to the root."""
    session = tmp_path / "-home-x-code-demo" / "abc12345-5985-491e-9ac2-af94587b18ab"
    subs = session / "subagents"
    subs.mkdir(parents=True)
    (subs / "agent-child01.meta.json").write_text(json.dumps(
        {"agentType": "general-purpose", "description": "the child probe",
         "toolUseId": "tu-child", "spawnDepth": 1}))
    (subs / "agent-child01.jsonl").write_text(_assistant("claude-sonnet-5", "tu-gc") + "\n")
    (subs / "agent-gc000002.meta.json").write_text(json.dumps(
        {"agentType": "general-purpose", "description": "the grandchild probe",
         "toolUseId": "tu-gc", "spawnDepth": 2}))
    (subs / "agent-gc000002.jsonl").write_text(_assistant("claude-haiku-4-5-20251001") + "\n")
    return session


def test_scan_reads_each_subagents_OWN_model(tmp_path: Path) -> None:
    by = {s.handle: s for s in scan_subagents(_write_swarm(tmp_path))}
    # the whole point: the model comes from the child's OWN transcript, not the parent's
    assert by["child01"].model == "claude-sonnet-5"
    assert by["gc000002"].model == "claude-haiku-4-5-20251001"
    assert by["child01"].spawn_depth == 1 and by["gc000002"].spawn_depth == 2
    assert by["gc000002"].description == "the grandchild probe"
    assert by["child01"].project == "demo"  # from the -home-x-code-demo dir name
    # each child ran ONE model → a single-entry history (no within-run swap)
    assert by["child01"].model_history == ("claude-sonnet-5",)
    assert by["gc000002"].model_history == ("claude-haiku-4-5-20251001",)


def test_resolve_parents_is_deterministic_from_tooluseid(tmp_path: Path) -> None:
    subs = scan_subagents(_write_swarm(tmp_path))
    parents = resolve_parents(subs)
    # grandchild's toolUseId (tu-gc) was emitted by the child's transcript → child is the parent
    assert parents["agent:gc000002"] == "agent:child01"
    # child's toolUseId (tu-child) emitted by no sibling → the root session spawned it
    assert parents["agent:child01"] == _ROOT


async def test_register_swarm_wires_tree_model_and_authority(
    actions: Actions, tmp_path: Path
) -> None:
    # the root mounted with a principal, so AUTHORITY (acts_for) can flow onto the swarm
    root = await actions.create_or_find_object("Agent", _ROOT, _ROOT)
    principal = await actions.create_or_find_object("Person", "principal:analyst:op", _ROOT)
    await actions.create_link(root, principal, "acts_for", _ROOT, NOW, 0.9,
                              evidence_class="self_declared")

    counts = await register_swarm(actions, _write_swarm(tmp_path))
    assert counts["agents"] == 2 and counts["spawned_by"] == 2

    # each sub-agent individuated with its TRUE model, graded as an OBSERVATION (not a guess)
    gc = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='source_model') AS model, "
        " (SELECT evidence_class FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='source_model') AS ec, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='spawn_depth') AS depth "
        "FROM objects o WHERE o.canonical='agent:gc000002'")
    assert gc["model"] == "claude-haiku-4-5-20251001"
    assert gc["ec"] == EvidenceClass.DIRECT_OBSERVATION.value
    assert gc["depth"] == "2"

    async def _target(canon: str, ltype: str) -> str | None:
        return await actions.pool.fetchval(
            "SELECT p.canonical FROM links l JOIN objects c ON c.id=l.from_id "
            "JOIN objects p ON p.id=l.to_id WHERE c.canonical=$1 AND l.type=$2", canon, ltype)

    # DELEGATION: grandchild → child → root (the fractal tree, from the path/tool_use links)
    assert await _target("agent:gc000002", "spawned_by") == "agent:child01"
    assert await _target("agent:child01", "spawned_by") == _ROOT
    # AUTHORITY (a DISTINCT edge): each sub-agent acts_for the root principal
    assert await _target("agent:gc000002", "acts_for") == "principal:analyst:op"


async def test_register_swarm_is_idempotent(actions: Actions, tmp_path: Path) -> None:
    session = _write_swarm(tmp_path)
    r1 = await register_swarm(actions, session)
    r2 = await register_swarm(actions, session)
    # re-scan re-processes both agents; but the second run creates NO new edges (idempotent) —
    # the invariant is the graph state, not the per-run counts (which report what CHANGED).
    assert r1["agents"] == r2["agents"] == 2
    assert r1["spawned_by"] == 2 and r2["spawned_by"] == 0
    # the two sub-agents don't duplicate across runs (root stub is minted once as the parent)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' "
        "AND canonical IN ('agent:child01','agent:gc000002')") == 2
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='spawned_by'") == 2


async def test_sense_swarms_walks_every_session(actions: Actions, tmp_path: Path) -> None:
    _write_swarm(tmp_path)  # one session with a subagents/ tree under the projects root
    counts = await sense_swarms(actions, tmp_path)
    assert counts["agents"] == 2
    # empty on a second pass? no — idempotent re-registration still reports what it saw
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical LIKE 'agent:gc%'") == 1


async def test_register_swarm_records_last_active_for_lifecycle(
    actions: Actions, tmp_path: Path
) -> None:
    """Lifecycle (#4): each sub-agent carries last_active (its transcript's mtime) so fleet() can
    show ● live vs ○ historical, not a growing pile of dead ephemerals all reading present."""
    await register_swarm(actions, _write_swarm(tmp_path))
    la = await actions.pool.fetchval(
        "SELECT value#>>'{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:child01' AND a.name='last_active'")
    assert la is not None
    datetime.fromisoformat(la)  # a parseable ISO timestamp (raises if not)


async def test_register_swarm_flags_within_session_subagent_swap(
    actions: Actions, tmp_path: Path
) -> None:
    """Task 4: a sub-agent demoted mid-run (>1 model in its OWN transcript) showed no swap when
    only the LATEST model was stamped. register_swarm now detects the transition and stamps
    model_swapped — within-session only (a swarm node has no operator-intent to diverge from)."""
    session = tmp_path / "-home-x-code-demo" / "abc12345-9999-491e-9ac2-af94587b18ab"
    subs = session / "subagents"
    subs.mkdir(parents=True)
    (subs / "agent-swap0001.meta.json").write_text(json.dumps(
        {"agentType": "general-purpose", "description": "the demoted probe",
         "toolUseId": "tu-s", "spawnDepth": 1}))
    # sonnet → opus mid-run: two assistant turns, distinct models (a warm rug-pull on the child)
    (subs / "agent-swap0001.jsonl").write_text(
        _assistant("claude-sonnet-5") + "\n" + _assistant("claude-opus-4-8") + "\n")

    await register_swarm(actions, session)

    row = await actions.pool.fetchrow(
        "SELECT (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='model_swapped') AS swapped, "
        " (SELECT evidence_class FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='model_swapped') AS ec, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='source_model') AS model "
        "FROM objects o WHERE o.canonical='agent:swap0001'")
    assert row["model"] == "claude-opus-4-8"  # the current (latest) turn
    assert row["swapped"] == "claude-sonnet-5 ↔ claude-opus-4-8 (now claude-opus-4-8)"
    assert row["ec"] == EvidenceClass.DIRECT_OBSERVATION.value  # read off the child's transcript


async def test_register_swarm_no_false_swap_on_single_model(
    actions: Actions, tmp_path: Path
) -> None:
    """A sub-agent that ran ONE model gets NO model_swapped — no cry-wolf on the swarm."""
    await register_swarm(actions, _write_swarm(tmp_path))  # child01 + gc, each a single model
    swapped = await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE a.name='model_swapped' AND o.canonical IN ('agent:child01','agent:gc000002')")
    assert swapped == 0


def _assistant_tool(model: str, tool_name: str) -> str:
    """An assistant line emitting one NON-spawn tool_use (an own act, e.g. Bash) plus text."""
    content = [{"type": "tool_use", "id": "t1", "name": tool_name, "input": {}},
               {"type": "text", "text": "ok"}]
    return json.dumps({"type": "assistant", "message": {"model": model, "content": content}})


def test_backed_by_observation_distinguishes_look_from_hearsay(tmp_path: Path) -> None:
    """Tier-1 act-detection (ruling 108ff2e8): an agent that ran its OWN tool looked; one whose
    only tool_use is an Agent spawn merely heard a child. The credence rebuttal reads this."""
    session = tmp_path / "-home-x-code-demo" / "abc12345-2222-491e-9ac2-af94587b18ab"
    subs = session / "subagents"
    subs.mkdir(parents=True)
    # a LOOKER — ran its own Bash (an own-observation act)
    (subs / "agent-looker01.meta.json").write_text(json.dumps(
        {"agentType": "x", "description": "d", "toolUseId": "tl", "spawnDepth": 1}))
    (subs / "agent-looker01.jsonl").write_text(_assistant_tool("claude-sonnet-5", "Bash") + "\n")
    # a HEARSAY agent — its only tool_use is an Agent spawn; it never looked, only heard a child
    (subs / "agent-hears002.meta.json").write_text(json.dumps(
        {"agentType": "x", "description": "d", "toolUseId": "th", "spawnDepth": 1}))
    (subs / "agent-hears002.jsonl").write_text(_assistant("claude-opus-4-8", "tu-x") + "\n")
    by = {s.handle: s for s in scan_subagents(session)}
    assert by["looker01"].backed_by_observation is True
    assert by["hears002"].backed_by_observation is False


async def test_sense_swarms_skips_unchanged_trees(actions: Actions, tmp_path: Path) -> None:
    """The mtime watermark (crunch residual): an unchanged subagents/ tree is not re-read on
    the next pass — real IO at fleet scale — and a touched tree re-registers."""
    import os
    import time as _t

    session = _write_swarm(tmp_path)
    first = await sense_swarms(actions, tmp_path)
    assert first["agents"] > 0 and first["skipped_unchanged"] == 0
    # second pass, nothing changed → the tree is skipped whole
    second = await sense_swarms(actions, tmp_path)
    assert second["skipped_unchanged"] == 1 and second["agents"] == 0
    # a touched transcript re-arms the pass (idempotent re-register)
    victim = next((session / "subagents").glob("agent-*.jsonl"))
    later = _t.time() + 60
    os.utime(victim, (later, later))
    third = await sense_swarms(actions, tmp_path)
    assert third["skipped_unchanged"] == 0 and third["agents"] > 0
