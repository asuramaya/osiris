"""THE READ TRIANGLE, WAVE 2 (thread 68f1bafa/3703a3a9): the `team` MCP tool -- a
manager's own seats (live, owe, envelope)."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, ensure_seat


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_team_lists_managed_seats_with_live_owe_envelope(actions: Actions) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        await _team_scenario(actions, srv, AgentIdentity)
    finally:
        srv._pool = saved_pool


async def _team_scenario(actions: Actions, srv, AgentIdentity) -> None:  # noqa: N803
    manager = await ensure_seat(actions, house="teamhouse", handle="Teammgr",
                                source="test", anchor_cwd="/test/teammgr")
    await actions.create_or_find_object("Agent", "agent:team-mgr-vii", "test")
    await bind_holder(actions, seat_id=manager["seat_id"], agent_id="agent:team-mgr-vii")
    await save_mount(actions.pool, job_dir="/jobs/teammgr", agent_id="agent:team-mgr-vii",
                     project="teamhouse", cwd="/test/teammgr", model="claude-sonnet-5",
                     session_key=None)

    # a LIVE worker, with one open + one past-window (stale) obligation, and an unread ask
    live_worker = await ensure_seat(actions, house="teamhouse", handle="Teamlive",
                                    source="test", anchor_cwd="/test/teamlive")
    await actions.create_or_find_object("Agent", "agent:team-live-vii", "test")
    await bind_holder(actions, seat_id=live_worker["seat_id"], agent_id="agent:team-live-vii")
    live_worker_oid = await actions.create_or_find_object(
        "Seat", live_worker["seat_id"], "test")
    manager_oid = await actions.create_or_find_object("Seat", manager["seat_id"], "test")
    await actions.create_link(live_worker_oid, manager_oid, "managed_by",
                              "test", datetime.now(UTC), 0.9, evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/teamlive", agent_id="agent:team-live-vii",
                     project="teamhouse", cwd="/test/teamlive", model="claude-sonnet-5",
                     session_key=None)
    await open_thread(actions, "an open duty for Teamlive", kind="obligation",
                      owner="Teamlive", repo="teamhouse", source="agent:team-live-vii")
    await open_thread(actions, "a stale duty for Teamlive", kind="obligation",
                      owner="Teamlive", repo="teamhouse", source="agent:team-live-vii",
                      stale_after_days=-1)
    send_ctx = _Ctx()
    srv._agents[srv._conn_key(send_ctx)] = AgentIdentity(
        agent_id="agent:team-sender", session="teamsender", project="teamhouse", model=None,
        cwd=None)
    try:
        await srv.send("need your word on this", to_agent="agent:team-live-vii", grade="ask",
                       ctx=send_ctx)
    finally:
        srv._agents.pop(srv._conn_key(send_ctx), None)

    # a COLD worker (held, nobody live), no obligations
    cold_worker = await ensure_seat(actions, house="teamhouse", handle="Teamcold",
                                    source="test", anchor_cwd="/test/teamcold")
    await actions.create_or_find_object("Agent", "agent:team-cold-vii", "test")
    await bind_holder(actions, seat_id=cold_worker["seat_id"], agent_id="agent:team-cold-vii")
    cold_worker_oid = await actions.create_or_find_object(
        "Seat", cold_worker["seat_id"], "test")
    await actions.create_link(cold_worker_oid, manager_oid, "managed_by",
                              "test", datetime.now(UTC), 0.9, evidence_class="self_declared")

    ctx = _Ctx()
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:team-mgr-vii", session="teammgr", project="teamhouse", model=None,
        cwd=None)
    try:
        out = await srv.team(ctx=ctx)
    finally:
        srv._agents.pop(srv._conn_key(ctx), None)

    rows = {r["handle"]: r for r in out["team"]}
    assert out["manager"] == "Teammgr"
    assert rows["Teamlive"]["live"] is True
    assert rows["Teamlive"]["owe"] == 2
    assert rows["Teamlive"]["stale"] == 1
    assert rows["Teamlive"]["envelope"] == 1
    assert rows["Teamcold"]["live"] is False
    assert rows["Teamcold"]["owe"] == 0
    assert rows["Teamcold"]["envelope"] == 0
    assert "Teammgr" not in rows  # the manager's own seat is not its own team


async def test_team_refuses_cleanly_when_the_caller_manages_nobody(actions: Actions) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    lone = await ensure_seat(actions, house="teamlonehouse", handle="Teamlone",
                             source="test", anchor_cwd="/test/teamlone")
    await actions.create_or_find_object("Agent", "agent:team-lone-vii", "test")
    await bind_holder(actions, seat_id=lone["seat_id"], agent_id="agent:team-lone-vii")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:team-lone-vii", session="teamlone", project="teamlonehouse",
        model=None, cwd=None)
    try:
        out = await srv.team(ctx=ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "error" in out


async def test_team_render_text(actions: Actions) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    manager = await ensure_seat(actions, house="teamtxthouse", handle="Teamtxtmgr",
                                source="test", anchor_cwd="/test/teamtxtmgr")
    await actions.create_or_find_object("Agent", "agent:team-txt-mgr-vii", "test")
    await bind_holder(actions, seat_id=manager["seat_id"], agent_id="agent:team-txt-mgr-vii")
    worker = await ensure_seat(actions, house="teamtxthouse", handle="Teamtxtwk",
                               source="test", anchor_cwd="/test/teamtxtwk")
    await actions.create_or_find_object("Agent", "agent:team-txt-wk-vii", "test")
    await bind_holder(actions, seat_id=worker["seat_id"], agent_id="agent:team-txt-wk-vii")
    worker_oid = await actions.create_or_find_object("Seat", worker["seat_id"], "test")
    manager_oid = await actions.create_or_find_object("Seat", manager["seat_id"], "test")
    await actions.create_link(worker_oid, manager_oid, "managed_by", "test",
                              datetime.now(UTC), 0.9, evidence_class="self_declared")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:team-txt-mgr-vii", session="teamtxtmgr", project="teamtxthouse",
        model=None, cwd=None)
    try:
        out = await srv.team(render="text", ctx=ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    assert set(out.keys()) == {"text"}
    assert "Teamtxtwk" in out["text"]
    assert "owe 0" in out["text"]
