"""lift(ref, handle) — extract a named, quiet rogue into a clean osiris office (thread 67f11cbd)."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import claim_name
from src.orchestrator.lift import lift
from src.orchestrator.mounts import save_mount


async def _rogue(
    actions: Actions, agent_id: str, cwd: str, *, project: str = "rogueproject",
    handle: str | None = None, live: bool = False,
) -> None:
    """A mounted lineage in some ad hoc cwd — the pre-lift shape. Quiet by default (the
    ceremony refuses a live one); `live=True` re-warms it for the refusal tests."""
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(a, "project", project, agent_id, now, 0.9,
                                  evidence_class="self_declared")
    if handle:
        await actions.assert_property(a, "handle", handle, agent_id, now, 0.9,
                                      evidence_class="self_declared")
    await save_mount(actions.pool, job_dir=f"/jobs/{agent_id.split(':')[-1]}", agent_id=agent_id,
                     project=project, cwd=cwd, model=None, session_key=None)
    if not live:
        await actions.pool.execute(
            "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' WHERE agent_id=$1",
            agent_id)


async def test_lift_the_whole_ceremony(actions: Actions, tmp_path: Path) -> None:
    cwd = str(tmp_path / "clusterfuck")
    Path(cwd).mkdir()
    await _rogue(actions, "agent:rogue0001", cwd)

    out = await lift(
        actions.pool, "agent:rogue0001", "Scribble", actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    office = tmp_path / "seats" / "scribble"
    assert out["agent"] == "agent:rogue0001"
    assert out["claimed"]["claimed"] == "Scribble"
    assert out["established"]["office"] == str(office)
    assert out["verified"] == {
        "resolved": True, "seat_bound": True, "handle_matches": True, "cwd_is_office": True,
    }
    assert out["post_lift"]["cwd"] == str(office)
    row = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1", "agent:rogue0001")
    assert row == str(office)
    assert (office / "CLAUDE.md").is_file()               # the standing orders really landed


async def test_lift_is_idempotent(actions: Actions, tmp_path: Path) -> None:
    """Re-running converges on the same office — claim_name re-affirms the same name,
    establish_office leaves the standing orders in place, exactly like establish_office
    alone. lift() adds nothing that breaks that."""
    cwd = str(tmp_path / "clusterfuck")
    Path(cwd).mkdir()
    await _rogue(actions, "agent:rogue0001", cwd)
    kwargs = {"office_root": tmp_path / "seats", "projects_root": tmp_path / "projects",
              "claude_json": tmp_path / "cj.json"}

    first = await lift(actions.pool, "agent:rogue0001", "Scribble", **kwargs)
    second = await lift(actions.pool, "agent:rogue0001", "Scribble", **kwargs)

    assert first["established"]["standing_orders"] == "written"
    assert second["established"]["standing_orders"].startswith("left in place")
    assert second["verified"] == first["verified"]


async def test_lift_refuses_an_unresolved_ref(actions: Actions, tmp_path: Path) -> None:
    out = await lift(actions.pool, "agent:ghost0001", "Nobody",
                     office_root=tmp_path / "seats")
    assert "error" in out
    assert "no such" in out["error"]
    assert not (tmp_path / "seats").exists()               # a refusal writes NOTHING


async def test_lift_refuses_an_ambiguous_cwd(actions: Actions, tmp_path: Path) -> None:
    shared = str(tmp_path / "shared")
    Path(shared).mkdir()
    await _rogue(actions, "agent:tenanta1", shared)
    await _rogue(actions, "agent:tenantb2", shared)

    out = await lift(actions.pool, shared, "Whoever", office_root=tmp_path / "seats")
    assert "error" in out
    assert "ambiguous" in out["error"]
    assert set(out["candidates"]) == {"agent:tenanta1", "agent:tenantb2"}
    assert not (tmp_path / "seats").exists()


async def test_lift_refuses_a_live_target(actions: Actions, tmp_path: Path) -> None:
    cwd = str(tmp_path / "livecwd")
    Path(cwd).mkdir()
    await _rogue(actions, "agent:livrogue1", cwd, live=True)

    out = await lift(actions.pool, "agent:livrogue1", "TooSoon",
                     office_root=tmp_path / "seats")
    assert "error" in out
    assert "LIVE right now" in out["error"]
    assert not (tmp_path / "seats").exists()


async def test_lift_propagates_claim_name_refusals(actions: Actions, tmp_path: Path) -> None:
    """A name already LIVE-held by someone else refuses at claim_name, and lift stops there
    — never partially establishes an office under the wrong name."""
    await _rogue(actions, "agent:holder001", "/w/holder", live=True)
    await claim_name(actions, "agent:holder001", "Taken", source="test")

    cwd = str(tmp_path / "rogue2")
    Path(cwd).mkdir()
    await _rogue(actions, "agent:rogue0002", cwd)

    out = await lift(actions.pool, "agent:rogue0002", "Taken", office_root=tmp_path / "seats")
    assert out["step"] == "claim_name"
    assert "currently held by" in out["error"]
    assert not (tmp_path / "seats").exists()


# ═══════════ THE MCP TOOL LAYER ═══════════
# test_mintseat.py's ritual is the precedent: fake a mounted connection by injecting an
# AgentIdentity into srv._agents keyed by srv._conn_key(ctx), point srv._pool at the test
# DB, call the tool FUNCTION directly (never the MCP transport). lift() exposes no
# office_root param (a live caller never gets to redirect where an office lands) — the
# happy-path test monkeypatches offices._DEFAULT_OFFICE_ROOT instead, exactly mint_seat's
# own technique for the same problem.

class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_mcp_lift_refuses_an_unmounted_caller(actions: Actions) -> None:
    import src.mcp_server as srv

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.lift(ref="agent:whoever", handle="Whoever", ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "error" in out and "mount" in out["error"]


async def test_mcp_lift_the_happy_path_through_the_tool_layer(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tool layer end to end: a mounted caller lifts a different, quiet rogue — the
    wrapper resolves `actor` from the connection's own identity (never a param) and hands
    off to the real lift(), which really writes the office (redirected to a scratch dir,
    since the tool exposes no office_root override)."""
    import src.mcp_server as srv
    from src.orchestrator import offices
    from src.orchestrator.agents import AgentIdentity

    monkeypatch.setattr(offices, "_DEFAULT_OFFICE_ROOT", tmp_path / "seats")
    cwd = str(tmp_path / "clusterfuck")
    Path(cwd).mkdir()
    await _rogue(actions, "agent:rogue0009", cwd)

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:caller001", session="caller001", project="osiris",
        model=None, cwd=None)
    try:
        out = await srv.lift(ref="agent:rogue0009", handle="Scribble", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "error" not in out
    assert out["agent"] == "agent:rogue0009"
    assert out["verified"]["seat_bound"] is True
    assert out["established"]["office"] == str(tmp_path / "seats" / "scribble")
