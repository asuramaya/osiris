"""THE READ TRIANGLE, WAVE 2 (thread 68f1bafa/3703a3a9): inbox(render='text') -- one line
per ASK message, FYI folded into a single trailing count line."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _seed(pool, project: str) -> None:
    await save_mount(pool, job_dir=f"/test/mailtext/{project}", agent_id=f"agent:seed-{project}",
                     project=project, cwd="/test", model=None, session_key=None)


async def test_inbox_render_text_folds_fyi_and_itemizes_asks(actions: Actions) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    proj = "mailtextbox"
    await _seed(actions.pool, proj)

    send_ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(send_ctx)] = AgentIdentity(
        agent_id="agent:mt-sender", session="mtsender", project=proj, model=None, cwd=None)
    try:
        await srv.send("an ask that needs a reply", to=proj, grade="ask", ctx=send_ctx)
        for i in range(3):
            await srv.send(f"fyi chatter {i}", to=proj, grade="fyi", ctx=send_ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(send_ctx), None)

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.inbox(project=proj, peek=True, render="text", ctx=ctx)
    finally:
        srv._pool = saved_pool

    assert set(out.keys()) == {"text"}
    lines = out["text"].splitlines()
    ask_lines = [line for line in lines if "ask from:" in line]
    assert len(ask_lines) == 1
    assert "an ask that needs a reply" in ask_lines[0]
    assert any("3 fyi message(s)" in line for line in lines)


async def test_inbox_render_text_on_an_empty_box(actions: Actions) -> None:
    from src import mcp_server as srv

    proj = "mailtextempty"
    await _seed(actions.pool, proj)

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.inbox(project=proj, peek=True, render="text", ctx=ctx)
    finally:
        srv._pool = saved_pool

    assert set(out.keys()) == {"text"}
    assert out["text"].splitlines()[0] == "mail: empty"
