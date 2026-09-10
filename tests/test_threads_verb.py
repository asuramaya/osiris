"""THE READ TRIANGLE, WAVE 2 (thread 68f1bafa/3703a3a9): the `threads` MCP tool -- MINE,
one line per OPEN thread you own (any owner_refs spelling), single-project, capped in
text mode."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.mounts import save_mount


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _mount(pool, *, agent_id: str, project: str, session: str) -> None:
    await save_mount(pool, job_dir=f"/test/threads/{session}", agent_id=agent_id,
                     project=project, cwd="/test", model=None, session_key=None)


async def test_threads_returns_only_the_callers_own_open_threads_in_project(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await open_thread(actions, "mine, in thproja", kind="obligation",
                      owner="agent:th-caller", repo="thproja", source="agent:th-caller")
    await open_thread(actions, "someone else's, in thproja", kind="obligation",
                      owner="agent:th-other", repo="thproja", source="agent:th-other")
    await open_thread(actions, "mine, but a different project", kind="obligation",
                      owner="agent:th-caller", repo="thprojb", source="agent:th-caller")
    await _mount(actions.pool, agent_id="agent:th-caller", project="thproja",
                session="thcaller")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:th-caller", session="thcaller", project="thproja", model=None,
        cwd=None)
    try:
        out = await srv.threads(ctx=ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["project"] == "thproja"
    assert out["total"] == 1
    assert out["threads"][0]["summary"] == "mine, in thproja"
    assert len(out["threads"][0]["id"]) == 8


async def test_threads_matches_a_handle_owner_not_just_the_literal_agent_id(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="osiris", handle="Thhandle1", source="test",
                             anchor_cwd="/test/thhandle")
    await actions.create_or_find_object("Agent", "agent:th-handle-vii", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:th-handle-vii")
    await open_thread(actions, "owned by the bare handle", kind="obligation",
                      owner="Thhandle1", repo="thprojc", source="agent:th-handle-vii")
    await _mount(actions.pool, agent_id="agent:th-handle-vii", project="thprojc",
                session="thhandlecaller")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:th-handle-vii", session="thhandlecaller", project="thprojc",
        model=None, cwd=None)
    try:
        out = await srv.threads(ctx=ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["total"] == 1
    assert out["threads"][0]["summary"] == "owned by the bare handle"


async def test_threads_marks_a_contested_thread_in_both_json_and_text(
    actions: Actions,
) -> None:
    """Fix (b), Metron's mechanism report (mail 8890/8921/8922): a thread whose newest
    note disputes its own summary carries contested=True in the JSON row and a leading
    `!` in the text render."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.capture import annotate_thread

    t = await open_thread(actions, "nothing renders the gain reduction", kind="obligation",
                          owner="agent:th-contested", repo="thprojcontested",
                          source="agent:th-contested")
    await annotate_thread(actions, str(t), "checked: MasterBand.tsx DOES render it")
    await _mount(actions.pool, agent_id="agent:th-contested", project="thprojcontested",
                session="thcontested")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:th-contested", session="thcontested", project="thprojcontested",
        model=None, cwd=None)
    try:
        out = await srv.threads(ctx=ctx)
        text_out = await srv.threads(ctx=ctx, render="text")
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["threads"][0]["contested"] is True
    assert text_out["text"].startswith("! ")


async def test_threads_render_text_returns_only_a_text_field_capped_with_remainder(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.textrender import THREADS_BAND_CAP

    for i in range(THREADS_BAND_CAP + 2):
        await open_thread(actions, f"duty {i} in thprojmany", kind="obligation",
                          owner="agent:th-many", repo="thprojmany", source="agent:th-many")
    await _mount(actions.pool, agent_id="agent:th-many", project="thprojmany",
                session="thmany")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:th-many", session="thmany", project="thprojmany", model=None,
        cwd=None)
    try:
        out = await srv.threads(render="text", ctx=ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    assert set(out.keys()) == {"text"}
    lines = out["text"].splitlines()
    assert lines[-1].startswith("+") and "more thread" in lines[-1]
    assert len(lines) == THREADS_BAND_CAP + 1


async def test_threads_refuses_cleanly_when_unmounted_and_no_project_given(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.threads(ctx=ctx)
    finally:
        srv._pool = saved

    assert "error" in out
