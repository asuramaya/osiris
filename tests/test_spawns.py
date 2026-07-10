"""Live spawn provenance — the sidechain impersonation kill (2026-07-10).

A sub-agent shares its parent's $CLAUDE_JOB_DIR and MCP connection, so every osiris call it
made resolved as the PARENT: a probe child was greeted 'you are Thoth XVII, writes attributed
to you' (live repro). The anchor hook now stamps sidechain calls with the harness's own
agent_id, and these tests drive the server half: the stamp attributes writes to the CHILD —
registered spawned_by its parent under the miner's keying — and the parent's away-fold names
its spawns instead of leaving it surprised.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.lineage import normalize_spawn_id, register_spawn
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 10, tzinfo=UTC)
_SD = EvidenceClass.SELF_DECLARED.value


async def _prop(actions: Actions, canonical: str, name: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name=$2 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canonical, name)


def test_normalize_spawn_id_converges_hook_and_miner_keying() -> None:
    """Hook payloads say 'agent-a932dd…', transcript filenames key the bare handle — both
    must land on the same Agent object."""
    assert normalize_spawn_id("agent-a932dd550cb8c9a30") == "a932dd550cb8c9a30"
    assert normalize_spawn_id("a932dd550cb8c9a30") == "a932dd550cb8c9a30"
    assert normalize_spawn_id("  agent-x  ") == "x"
    assert normalize_spawn_id("agent-") is None
    assert normalize_spawn_id("") is None and normalize_spawn_id(None) is None


async def test_register_spawn_wires_child_parent_and_authority(actions: Actions) -> None:
    parent = await actions.create_or_find_object("Agent", "agent:par00001", "agent:par00001")
    person = await actions.create_or_find_object("Person", "principal:op", "agent:par00001")
    await actions.create_link(parent, person, "acts_for", "agent:par00001", NOW, 0.9,
                              evidence_class=_SD)
    child = await register_spawn(
        Actions(actions.pool), "agent-kid00001", agent_type="Explore",
        parent_agent="agent:par00001", project="demo", session="par00001")
    assert child == "agent:kid00001"
    assert await _prop(actions, child, "is_sidechain") == "true"
    assert await _prop(actions, child, "agent_type") == "Explore"
    assert await _prop(actions, child, "project") == "demo"
    edges = await actions.pool.fetch(
        "SELECT l.type, t.canonical AS target FROM links l "
        "JOIN objects c ON c.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE c.canonical=$1 ORDER BY l.type", child)
    by_type = {r["type"]: r["target"] for r in edges}
    assert by_type["spawned_by"] == "agent:par00001"   # delegation → the direct parent
    assert by_type["acts_for"] == "principal:op"       # authority → the parent's principal
    assert by_type["works_in"] == "repo:demo"
    # idempotent: a second registration adds no duplicate edges
    await register_spawn(Actions(actions.pool), "kid00001", agent_type="Explore",
                         parent_agent="agent:par00001", project="demo")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects c ON c.id=l.from_id "
        "WHERE c.canonical=$1 AND l.type='spawned_by'", child) == 1


async def test_register_spawn_stop_reads_the_childs_own_model(
    actions: Actions, tmp_path: Path
) -> None:
    """SubagentStop hands the child's transcript — its OWN model lands (the whole point of
    the swarm layer: a Haiku child never records as its Opus parent), plus the done stamp."""
    t = tmp_path / "agent-kid00002.jsonl"
    t.write_text(json.dumps({"type": "assistant",
                             "message": {"model": "claude-haiku-4-5", "content": []}}) + "\n")
    child = await register_spawn(Actions(actions.pool), "kid00002", agent_type="claude",
                                 transcript=t, done=True)
    assert child == "agent:kid00002"
    assert await _prop(actions, child, "source_model") == "claude-haiku-4-5"
    assert await _prop(actions, child, "last_active") is not None


async def test_actor_for_attributes_the_stamped_child_never_the_seat(
    actions: Actions,
) -> None:
    """The write waist: a hook-stamped call lands on the CHILD; an unstamped call falls
    through to the normal (mounted / session) resolution."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        actor = await srv._actor_for(None, "agent-kid00003", "code-reviewer")
        assert actor == "agent:kid00003"
        assert await _prop(actions, actor, "agent_type") == "code-reviewer"
        assert await srv._actor_for(None, None) == "session"  # unstamped → normal path
        # the write tools carry the stamp end to end
        out = await srv.record_decision("spawn-made ruling", kind="choice",
                                        subagent_id="agent-kid00003")
        src_of = await actions.pool.fetchval(
            "SELECT a.source_id FROM assertions a JOIN objects o ON o.id=a.object_id "
            "WHERE o.id=$1 AND a.name='summary' LIMIT 1", __import__("uuid").UUID(out["id"]))
        assert src_of == "agent:kid00003"  # the child's word, under the child's name
    finally:
        srv._pool = saved_pool
        srv._spawns_seen.clear()


async def test_mount_by_a_spawn_never_takes_the_seat(actions: Actions, tmp_path: Path) -> None:
    """A spawn's mount() is a REGISTRATION, not a seat claim: no durable row, no hot-cache
    write, and the response says plainly whose child it is."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "beef0001")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:beef0001",
                            project="osiris", cwd=str(tmp_path), model=None,
                            session_key=None)  # the PARENT's row — must stay untouched
    saved_pool = srv._pool
    srv._pool = actions.pool
    agents_before = dict(srv._agents)
    try:
        out = await srv.mount(cwd=str(tmp_path), job_dir=job_dir,
                              subagent_id="agent-kid00004", subagent_type="Explore")
        assert out["agent"] == "agent:kid00004"
        assert "SPAWN" in out["note"]
        rec = await mounts.find_mount(actions.pool, job_dir=job_dir)
        assert rec is not None and rec.agent_id == "agent:beef0001"  # seat untouched
        assert dict(srv._agents) == agents_before                    # cache untouched
    finally:
        srv._pool = saved_pool
        srv._spawns_seen.clear()


async def test_while_away_names_your_spawns(actions: Actions) -> None:
    """The surprise kill: a returning parent's away-fold lists the children its lineage
    spawned since its last sign of life — type, model, when."""
    since = datetime.now(UTC) - timedelta(hours=1)
    await actions.create_or_find_object("Agent", "agent:par00005-ii", "agent:par00005-ii")
    await register_spawn(Actions(actions.pool), "kid00005", agent_type="Plan",
                         parent_agent="agent:par00005-ii", project="demo")
    # the CALLER is a later generation — the fold still finds the base lineage's children
    away = await mounts.while_away(actions.pool, "demo", "agent:par00005-iii", since)
    assert away is not None
    spawns = away.get("spawns") or []
    assert any(s["agent"] == "agent:kid00005" and s["type"] == "Plan" for s in spawns)
