"""THE READ TRIANGLE: inbox(render='text') -- one line
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


# --- a glance must never lease what it only glanced at ------------------------------------------

_LONG_BODY = (
    "Design review for the next tip, three parts.\n\n"
    "1. Move the lease into the structured read only; the text render is a one-line glance.\n"
    "2. Keep the whole body, newlines and numbering intact, for the read that does lease.\n"
    "3. Say so on the glance when it was cut, and how much is missing.\n\n"
    "Reply only with new information.")


async def _send_ask(srv, pool, proj: str, body: str) -> None:  # noqa: ANN001
    from src.orchestrator.agents import AgentIdentity

    send_ctx = _Ctx()
    saved = srv._pool
    srv._pool = pool
    srv._agents[srv._conn_key(send_ctx)] = AgentIdentity(
        agent_id="agent:glance-sender", session="glancesender", project=proj, model=None,
        cwd=None)
    try:
        await srv.send(body, to=proj, grade="ask", ctx=send_ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(send_ctx), None)


async def test_a_text_render_without_peek_does_not_lease_and_says_it_was_cut(
    actions: Actions,
) -> None:
    """Two workers asked for resends of mail they had 'read' through the text render without
    peek: it leased the message (holding it back for the lease window) while showing one line
    of it. A text render is a glance now: nothing is leased, and a cut row says so."""
    from src import mcp_server as srv

    proj = "glancebox"
    await _seed(actions.pool, proj)
    await _send_ask(srv, actions.pool, proj, _LONG_BODY)

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    try:
        glance = await srv.inbox(project=proj, render="text", ctx=ctx)   # no peek
    finally:
        srv._pool = saved

    row = glance["text"].splitlines()[0]
    assert "Design review for the next tip" in row
    assert "\n" not in row and "more characters: inbox() reads it whole" in row
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE to_project=$1 AND leased_by IS NOT NULL",
        proj) == 0
    # and the whole message is still there to be read: the glance took nothing from it
    srv._pool = actions.pool
    try:
        whole = await srv.inbox(project=proj, ctx=ctx)
    finally:
        srv._pool = saved
    assert whole["messages"][0]["body"] == _LONG_BODY


async def test_the_whole_multi_line_body_is_delivered_by_the_read_after_the_nudge(
    actions: Actions,
) -> None:
    """The nudge only announces: a flattened preview inside a small box. The recipient then
    reads the message through inbox(), which must hand over the whole body, newlines and
    numbered list intact, and that read is the one that leases."""
    from src import mcp_server as srv
    from src.orchestrator.trigger import _mail_envelope

    proj = "wholebodybox"
    await _seed(actions.pool, proj)
    await _send_ask(srv, actions.pool, proj, _LONG_BODY)
    msg = await actions.pool.fetchrow(
        "SELECT id, grade, left(body, 160) AS preview FROM fleet_messages WHERE to_project=$1",
        proj)

    preview = " ".join(str(msg["preview"]).split())      # the nudge path's own flattening
    env = _mail_envelope(msg["id"], sender_label="sender", addressee_label="you", grade="ask",
                         preview=preview)
    assert len(env.splitlines()) == 6 and "inbox() reads it whole" in env
    assert "3. Say so" not in env                          # the envelope is only a preview

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.inbox(project=proj, ctx=ctx)
    finally:
        srv._pool = saved

    assert out["messages"][0]["body"] == _LONG_BODY
    # that whole read is the lease: asked again straight away, the message is held, not redelivered
    srv._pool = actions.pool
    try:
        again = await srv.inbox(project=proj, ctx=ctx)
    finally:
        srv._pool = saved
    assert again["messages"] == []
