"""inbox(as_seat=...) — the charter-gated, read-only cross-seat mail door (9dc3ce8b/
c56f3d94, MAIL IS UNSURFACEABLE). A coordinator can read another seat's RECEIVED DMs,
including already-settled ones, but only when it governs a project that seat also
charters — never a bare seat-to-seat peek, never a lease, never a settle.
"""
from __future__ import annotations

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import AgentIdentity
from src.orchestrator.charter import set_charter
from src.orchestrator.mailbox import send_message
from src.orchestrator.seats import bind_holder, ensure_seat


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _seated(actions: Actions, agent_id: str, handle: str) -> str:
    out = await ensure_seat(actions, house="osiris", handle=handle, source="test")
    seat_id = str(out["seat_id"])
    await bind_holder(actions, seat_id=seat_id, agent_id=agent_id)
    return seat_id


async def _repo(actions: Actions, name: str) -> None:
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")


async def _mount(agent_id: str) -> _Ctx:
    import src.mcp_server as srv

    ctx = _Ctx()
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent_id, session=agent_id, project=None, model=None, cwd=None)
    return ctx


@pytest.fixture
async def _pool(actions: Actions):
    import src.mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    yield actions.pool
    srv._pool = saved


async def test_as_seat_refuses_when_caller_does_not_govern_the_target_project(
    actions: Actions, _pool,
) -> None:
    from src.mcp_server import inbox as inbox_tool

    await _repo(actions, "closed-repo")
    coordinator_seat = await _seated(actions, "agent:coordinator1", "Coordinator1")
    await set_charter(actions, coordinator_seat, ["closed-repo"], actor="agent:steward")
    worker_seat = await _seated(actions, "agent:worker1", "Worker1")
    await _repo(actions, "other-repo")
    await set_charter(actions, worker_seat, ["other-repo"], actor="agent:steward")

    ctx = await _mount("agent:coordinator1")
    out = await inbox_tool(as_seat=worker_seat, ctx=ctx)
    assert "error" in out
    assert "do not govern" in out["error"]


async def test_as_seat_reads_dms_when_caller_governs_the_shared_project(
    actions: Actions, _pool,
) -> None:
    from src.mcp_server import inbox as inbox_tool

    await _repo(actions, "shared-repo")
    coordinator_seat = await _seated(actions, "agent:coordinator2", "Coordinator2")
    await set_charter(actions, coordinator_seat, ["shared-repo"], actor="agent:steward")
    worker_seat = await _seated(actions, "agent:worker2", "Worker2")
    await set_charter(actions, worker_seat, ["shared-repo"], actor="agent:steward")
    await send_message(actions.pool, from_agent="agent:boss", from_project="shared-repo",
                       to_agent="agent:worker2", body="finish the report")

    ctx = await _mount("agent:coordinator2")
    out = await inbox_tool(as_seat=worker_seat, ctx=ctx)
    assert out["target_agent"] == "agent:worker2"
    assert len(out["messages"]) == 1
    assert out["messages"][0]["body"] == "finish the report"
    assert out["messages"][0]["settled"] is False


async def test_as_seat_on_a_vacant_seat_is_empty_not_an_error(
    actions: Actions, _pool,
) -> None:
    from src.mcp_server import inbox as inbox_tool

    await _repo(actions, "shared-repo2")
    coordinator_seat = await _seated(actions, "agent:coordinator3", "Coordinator3")
    await set_charter(actions, coordinator_seat, ["shared-repo2"], actor="agent:steward")
    vacant = await ensure_seat(actions, house="osiris", handle="Vacant1", source="test")
    await set_charter(actions, str(vacant["seat_id"]), ["shared-repo2"],
                      actor="agent:steward")

    ctx = await _mount("agent:coordinator3")
    out = await inbox_tool(as_seat=str(vacant["seat_id"]), ctx=ctx)
    assert out["target_agent"] is None
    assert out["messages"] == []


async def test_as_seat_refuses_ack_alongside_it(actions: Actions, _pool) -> None:
    from src.mcp_server import inbox as inbox_tool

    await _repo(actions, "shared-repo3")
    coordinator_seat = await _seated(actions, "agent:coordinator4", "Coordinator4")
    await set_charter(actions, coordinator_seat, ["shared-repo3"], actor="agent:steward")
    worker_seat = await _seated(actions, "agent:worker4", "Worker4")
    await set_charter(actions, worker_seat, ["shared-repo3"], actor="agent:steward")

    ctx = await _mount("agent:coordinator4")
    out = await inbox_tool(as_seat=worker_seat, ack=[1], ctx=ctx)
    assert "error" in out
    assert "read-only" in out["error"]
