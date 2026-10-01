"""Every mailbox door reads the same scope for the same caller.

The doors: inbox (peek, lease, text), get_mail, the count line mount/orient print, the Stop
gate, and the CLI/hook read. They used to disagree: the Stop gate ran its own copy of the
deliverable predicate and left out mail addressed to a seat the caller holds, and the
hook-served `/mail` read the project's broadcast scope for nobody in particular, so it
reported a pile of broadcast notices while the caller's own direct mail sat unseen.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from src.actions.core import Actions
from src.cli import cmd_inbox
from src.orchestrator.agents import AgentIdentity
from src.orchestrator.mailbox import (
    deliverable_bands,
    read_inbox,
    send_message,
    unread_counts,
)
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, ensure_seat
from src.orchestrator.stophook_logic import compute_stop_deliverable
from src.parsers.base import EvidenceClass


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


@pytest.fixture
async def _pool(actions: Actions):
    import src.mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    yield actions.pool
    srv._pool = saved


async def _seated_caller(actions: Actions, agent: str, handle: str, project: str) -> str:
    """A mounted agent that holds a seat, with a mount row findable by session id."""
    obj = await actions.create_or_find_object("Agent", agent, agent)
    await actions.assert_property(obj, "project", project, agent, datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    seat = await ensure_seat(actions, house=project, handle=handle, source="test")
    await bind_holder(actions, seat_id=str(seat["seat_id"]), agent_id=agent)
    await save_mount(actions.pool, job_dir=f"/j/jobs/{handle.lower()[:8].ljust(8, 'x')}",
                     agent_id=agent, project=project, cwd="/scope/office",
                     model="claude-fable-5", session_key=None)
    return str(seat["seat_id"])


async def _mixed_mail(actions: Actions, agent: str, seat_id: str, project: str) -> None:
    """Three deliverable messages of three kinds: a DM to the agent, a DM to the seat it
    holds, and a broadcast to its project."""
    pool = actions.pool
    await send_message(pool, from_agent="agent:other", from_project=project,
                       to_agent=agent, body="direct", grade="ask")
    await send_message(pool, from_agent="agent:other", from_project=project,
                       to_agent=seat_id, body="to the seat", grade="ask")
    await send_message(pool, from_agent="agent:other", from_project=project,
                       to_project=project, body="broadcast", grade="fyi")


async def test_stop_gate_counts_mail_addressed_to_a_held_seat(actions: Actions) -> None:
    """The Stop gate's own hand-copied predicate used to skip seat-addressed mail, so a
    message inbox() showed never made the gate say a word."""
    agent, project = "agent:scopeseat1", "scopeprojone"
    seat_id = await _seated_caller(actions, agent, "Scopeone", project)
    await send_message(actions.pool, from_agent="agent:other", from_project=project,
                       to_agent=seat_id, body="to the seat", grade="ask")

    out = await compute_stop_deliverable(
        actions.pool, cwd="/scope/office", session_id="scopeone-0000-4000-8000-000000000000")
    assert out["n"] == 1
    assert out["bands"] == {"ask": 1, "fyi": 0}


async def test_every_count_door_agrees_for_the_same_caller(actions: Actions) -> None:
    agent, project = "agent:scopeseat2", "scopeprojtwo"
    seat_id = await _seated_caller(actions, agent, "Scopetwo", project)
    await _mixed_mail(actions, agent, seat_id, project)

    peeked = await read_inbox(actions.pool, project, reader_agent=agent, mark_read=False)
    counts = await unread_counts(actions.pool, project, reader_agent=agent)
    bands = await deliverable_bands(actions.pool, project, reader_agent=agent)
    stop = await compute_stop_deliverable(
        actions.pool, cwd="/scope/office", session_id="scopetwo-0000-4000-8000-000000000000")

    assert len(peeked) == counts["total"] == bands["n"] == stop["n"] == 3
    assert counts["ask"] == bands["ask"] == stop["bands"]["ask"] == 2
    assert bands["fyi"] == stop["bands"]["fyi"] == 1


async def test_inbox_names_a_mounted_callers_own_mailbox_scope(
    actions: Actions, _pool,
) -> None:
    import src.mcp_server as srv
    from src.mcp_server import inbox as inbox_tool

    agent, project = "agent:scopeseat3", "scopeprojthree"
    seat_id = await _seated_caller(actions, agent, "Scopethree", project)
    await _mixed_mail(actions, agent, seat_id, project)
    ctx = _Ctx()
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session=agent, project=project, model=None, cwd=None)

    try:
        out = await inbox_tool(peek=True, ctx=ctx)
        assert len(out["messages"]) == 3
        assert out["scope"].startswith("your mailbox")
        text = (await inbox_tool(peek=True, render="text", ctx=ctx))["text"]
        assert "scope:" not in text  # a caller's own mailbox is the default, not a line
    finally:
        srv._agents.pop(srv._conn_key(ctx), None)


async def test_inbox_says_when_it_read_the_project_broadcasts_only(
    actions: Actions, _pool,
) -> None:
    """An unresolved caller asking for a project sees broadcasts, never direct mail, and
    the output has to say so rather than let a short list pass for a clean inbox."""
    from src.mcp_server import inbox as inbox_tool

    agent, project = "agent:scopeseat4", "scopeprojfour"
    seat_id = await _seated_caller(actions, agent, "Scopefour", project)
    await _mixed_mail(actions, agent, seat_id, project)

    import src.mcp_server as srv

    ctx = _Ctx()  # nothing mounted on this connection
    srv._agents.pop(srv._conn_key(ctx), None)  # _Ctx's connection key is shared class-wide
    out = await inbox_tool(project=project, peek=True, ctx=ctx)
    assert [m["body"] for m in out["messages"]] == ["broadcast"]
    assert out["scope"].startswith(f"project broadcasts to {project} only")
    text = (await inbox_tool(project=project, peek=True, render="text", ctx=ctx))["text"]
    assert f"scope: project broadcasts to {project} only" in text
    assert "DMs not shown" in text


async def test_cli_inbox_with_a_session_reads_that_sessions_own_mailbox(
    monkeypatch: Any, capsys: Any,
) -> None:
    calls: list[dict[str, Any]] = []

    async def _fake_call(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append({"tool": name, **arguments})
        return {"text": "14695 ask from:agent:x thread:1: hello"}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _fake_call)
    monkeypatch.setattr("src.cli._mcp_url", _async_value("http://x"))
    assert await cmd_inbox(project="osiris", session="sessnid1-0000-4000-8000-000000000000",
                           text=True) == 0
    assert len(calls) == 1
    assert calls[0]["session_anchor"].endswith("/.claude/jobs/sessnid1")
    assert "project" not in calls[0]  # the session's own mailbox, not the project's
    assert "14695 ask" in capsys.readouterr().out


async def test_cli_inbox_falls_back_to_the_project_when_the_session_does_not_resolve(
    monkeypatch: Any,
) -> None:
    calls: list[dict[str, Any]] = []

    async def _fake_call(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append(dict(arguments))
        if "session_anchor" in arguments:
            return {"error": "mount(cwd, job_dir=<your anchor>) first, or pass project=<repo>"}
        return {"text": "mail: empty\nscope: project broadcasts to osiris only"}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _fake_call)
    monkeypatch.setattr("src.cli._mcp_url", _async_value("http://x"))
    assert await cmd_inbox(project="osiris", session="sessnid1-0000-4000-8000-000000000000",
                           text=True) == 0
    assert len(calls) == 2
    assert calls[1]["project"] == "osiris"
    assert "session_anchor" not in calls[1]


async def test_cli_inbox_needs_a_project_or_a_session(capsys: Any) -> None:
    assert await cmd_inbox(project=None, session=None) == 2
    assert "--project" in capsys.readouterr().err


def _async_value(value: str) -> Any:
    async def _inner() -> str:
        return value

    return _inner


async def test_a_peek_resolves_an_existing_mount_row_and_mints_nothing(
    actions: Actions, _pool,
) -> None:
    """A glance from a hook reads the identity a mount row already names; it never
    registers, mints or saves anything, and an anchor with no row is just unresolved."""
    from src.mcp_server import inbox as inbox_tool

    agent, project = "agent:scopeseat7", "scopeprojseven"
    seat_id = await _seated_caller(actions, agent, "Scopesev7", project)
    await _mixed_mail(actions, agent, seat_id, project)
    anchor = "/j/jobs/scopesev"  # the row _seated_caller saved for handle "Scopesev7"

    agents_before = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Agent'")
    mounts_before = await actions.pool.fetchval("SELECT count(*) FROM agent_mounts")
    seen = await inbox_tool(peek=True, session_anchor=anchor, ctx=_Ctx())
    assert len(seen["messages"]) == 3 and seen["scope"].startswith("your mailbox")

    unknown = await inbox_tool(peek=True, session_anchor="/j/jobs/nosuchrw",
                               project=project, ctx=_Ctx())
    assert unknown["scope"].startswith("project broadcasts")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent'") == agents_before
    assert await actions.pool.fetchval("SELECT count(*) FROM agent_mounts") == mounts_before
