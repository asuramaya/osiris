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


async def test_spawn_inbox_is_peek_only(actions: Actions, tmp_path: Path) -> None:
    """A spawn reading its parent's mailbox must never LEASE (a dying child's lease blocks
    redelivery) nor SETTLE (that is the seat's duty) — peek is FORCED, ack dropped."""
    from types import SimpleNamespace

    from src import mcp_server as srv
    from src.orchestrator.mailbox import send_message

    job_dir = str(tmp_path / "jobs" / "feed0007")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:feed0007",
                            project="demo", cwd=str(tmp_path), model=None,
                            session_key="sid:spawnbox")
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "cafe0008"),
                            agent_id="agent:cafe0008", project="demo", cwd=str(tmp_path),
                            model=None, session_key="sid:other")
    msg = await send_message(actions.pool, from_agent="agent:cafe0008", from_project="demo",
                             to_project="demo", body="work for the seat")
    ctx = SimpleNamespace(request_context=SimpleNamespace(
        request=SimpleNamespace(headers={"mcp-session-id": "spawnbox",
                                         "x-osiris-job": job_dir}), session=object()))
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.inbox(peek=False, ack=[int(msg["id"])],
                              subagent_id="agent-kid00006", ctx=ctx)
        assert "spawn read" in out["note"] and "peek FORCED" in out["note"]
        assert "settled" not in out                       # the ack was dropped, not honored
        leased = await actions.pool.fetchval(
            "SELECT count(*) FROM message_recipients WHERE message_id=$1 "
            "AND delivered_at IS NOT NULL", int(msg["id"]))
        assert leased == 0                                # nothing leased by the peek
    finally:
        srv._pool = saved_pool
        srv._agents.pop("sid:spawnbox", None)
        srv._spawns_seen.clear()


async def test_dm_to_a_spawn_warns_of_the_dead_letter(actions: Actions, tmp_path: Path) -> None:
    """A DM addressed to an ephemeral spawn may never be read — send() says so at send time
    and points at the parent seat."""
    from types import SimpleNamespace

    from src import mcp_server as srv

    await register_spawn(Actions(actions.pool), "kid00007", agent_type="Explore",
                         parent_agent="agent:beef0009", project="demo")
    job_dir = str(tmp_path / "jobs" / "beef0009")
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id="agent:beef0009",
                            project="demo", cwd=str(tmp_path), model=None,
                            session_key="sid:dmspawn")
    ctx = SimpleNamespace(request_context=SimpleNamespace(
        request=SimpleNamespace(headers={"mcp-session-id": "dmspawn",
                                         "x-osiris-job": job_dir}), session=object()))
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.send("are you there?", to_agent="agent:kid00007", ctx=ctx)
        assert out.get("sent") and "SPAWN" in out.get("warning", "")
        # a DM to a real (non-spawn) agent stays warning-free
        out2 = await srv.send("hello seat", to_agent="agent:beef0009", ctx=ctx)
        assert out2.get("sent") and "warning" not in out2
    finally:
        srv._pool = saved_pool
        srv._agents.pop("sid:dmspawn", None)
        srv._spawns_seen.clear()


async def test_register_spawn_never_testifies_above_what_it_witnessed(
    actions: Actions, tmp_path: Path
) -> None:
    """The ghost-spawn law (ruling 708a972d): the harness announces sidechains whose
    transcript never materializes. A named-but-absent transcript stamps the spawn
    unwitnessed; the file appearing (Stop) upgrades it; an observed ACT (witnessed=True)
    is never un-witnessed by an unflushed file."""
    ghost_path = tmp_path / "agent-ghost0001.jsonl"  # announced, never materialized
    child = await register_spawn(Actions(actions.pool), "ghost0001", agent_type="claude",
                                 transcript=ghost_path)
    assert child == "agent:ghost0001"
    assert await _prop(actions, child, "spawn_witnessed") == "false"
    # the transcript materializes (a real spawn's Stop): disk truth upgrades the stamp
    ghost_path.write_text(json.dumps({"type": "assistant",
                                      "message": {"model": "claude-haiku-4-5",
                                                  "content": []}}) + "\n")
    await register_spawn(Actions(actions.pool), "ghost0001", transcript=ghost_path, done=True)
    assert await _prop(actions, child, "spawn_witnessed") == "true"
    # an acting child (hook-stamped tool call) is witnessed even with no file yet
    kid = await register_spawn(Actions(actions.pool), "acting01",
                               transcript=tmp_path / "agent-acting01.jsonl", witnessed=True)
    assert kid is not None
    assert await _prop(actions, kid, "spawn_witnessed") == "true"
    # no transcript, no act flag → nothing stamped (unknown is not unwitnessed)
    quiet = await register_spawn(Actions(actions.pool), "quiet001")
    assert quiet is not None
    assert await _prop(actions, quiet, "spawn_witnessed") is None


async def test_while_away_calms_the_ghost_and_keeps_the_warning_for_hands(
    actions: Actions, tmp_path: Path
) -> None:
    """The away-fold's warning is reserved for WITNESSED hands: when the only arrivals are
    unwitnessed harness sidechains, the note says so calmly instead of 'another hand may
    have worn your face' (Maat's identity scare, bug 75c59aad)."""
    since = datetime.now(UTC) - timedelta(hours=1)
    await actions.create_or_find_object("Agent", "agent:par00009", "agent:par00009")
    await register_spawn(Actions(actions.pool), "ghost0009", agent_type="claude",
                         parent_agent="agent:par00009", project="demo-ghost",
                         transcript=tmp_path / "agent-ghost0009.jsonl")
    away = await mounts.while_away(actions.pool, "demo-ghost", "agent:par00009", since)
    assert away is not None
    (spawn,) = away["spawns"]
    assert spawn["agent"] == "agent:ghost0009"
    assert "unwitnessed" in spawn and "likely internal" in spawn["unwitnessed"]
    assert "another hand" not in away["note"]
    assert "unwitnessed harness sidechains" in away["note"]
    # a WITNESSED spawn arriving restores the full warning, and carries no ghost marker
    t = tmp_path / "agent-real0009.jsonl"
    t.write_text(json.dumps({"type": "assistant",
                             "message": {"model": "claude-haiku-4-5", "content": []}}) + "\n")
    await register_spawn(Actions(actions.pool), "real0009", agent_type="Explore",
                         parent_agent="agent:par00009", project="demo-ghost", transcript=t)
    away2 = await mounts.while_away(actions.pool, "demo-ghost", "agent:par00009", since)
    assert away2 is not None and "another hand" in away2["note"]
    by_id = {s["agent"]: s for s in away2["spawns"]}
    assert "unwitnessed" not in by_id["agent:real0009"]
    assert "unwitnessed" in by_id["agent:ghost0009"]


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
