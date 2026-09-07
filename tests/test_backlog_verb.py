"""THE READ TRIANGLE, WAVE 2 (thread 68f1bafa/3703a3a9): the `backlog` MCP tool -- a
standalone read verb over digest.py's own `_obligation_pressure`, scoped to the caller's
project by default with an `all_projects` widen, ordered caller's-project-first / then
past-window / then open-count, capped and folded in text mode.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.mounts import save_mount


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _mount(pool, *, agent_id: str, project: str, session: str) -> None:
    await save_mount(pool, job_dir=f"/test/backlog/{session}", agent_id=agent_id,
                     project=project, cwd="/test", model=None, session_key=None)


async def test_backlog_scopes_to_the_callers_own_project_by_default(actions: Actions) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await open_thread(actions, "a duty in blproja", kind="obligation", owner="Owner1",
                      repo="blproja", source="agent:seed-blproja")
    await open_thread(actions, "a duty in blprojb", kind="obligation", owner="Owner2",
                      repo="blprojb", source="agent:seed-blprojb")
    await _mount(actions.pool, agent_id="agent:bl-caller", project="blproja",
                session="blcaller")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:bl-caller", session="blcaller", project="blproja", model=None,
        cwd=None)
    try:
        out = await srv.backlog(ctx=ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["scope"] == "blproja"
    assert {r["project"] for r in out["projects"]} == {"blproja"}


async def test_backlog_all_projects_orders_own_project_first_then_past_window(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    # blprojc: not the caller's own, no past-window obligations, larger count
    for i in range(3):
        await open_thread(actions, f"duty {i} in blprojc", kind="obligation",
                          owner=f"Owner{i}", repo="blprojc", source="agent:seed-blprojc")
    # blprojd: not the caller's own, ONE past-window obligation, smaller count
    await open_thread(actions, "a stale duty in blprojd", kind="obligation", owner="OwnerD",
                      repo="blprojd", source="agent:seed-blprojd", stale_after_days=-1)
    # blprojown: the caller's own project, smallest count, no past window
    await open_thread(actions, "a duty in blprojown", kind="obligation", owner="OwnerE",
                      repo="blprojown", source="agent:seed-blprojown")
    await _mount(actions.pool, agent_id="agent:bl-caller2", project="blprojown",
                session="blcaller2")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:bl-caller2", session="blcaller2", project="blprojown", model=None,
        cwd=None)
    try:
        out = await srv.backlog(all_projects=True, ctx=ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(ctx), None)

    projects = [r["project"] for r in out["projects"]
               if r["project"] in ("blprojc", "blprojd", "blprojown")]
    assert projects == ["blprojown", "blprojd", "blprojc"]
    assert out["scope"] == "all"


async def test_backlog_render_text_returns_only_a_text_field_capped_with_remainder(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.textrender import BACKLOG_BAND_CAP

    for i in range(BACKLOG_BAND_CAP + 3):
        await open_thread(actions, f"duty in blmany{i}", kind="obligation", owner="Owner",
                          repo=f"blmany{i}", source=f"agent:seed-blmany{i}")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.backlog(all_projects=True, render="text", ctx=ctx)
    finally:
        srv._pool = saved

    assert set(out.keys()) == {"text"}
    lines = out["text"].splitlines()
    assert lines[-1].startswith("+") and "more project" in lines[-1]
    assert len(lines) == BACKLOG_BAND_CAP + 1
