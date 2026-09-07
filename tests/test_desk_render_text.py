"""THE READ TRIANGLE, WAVE 2 (thread 68f1bafa/3703a3a9): inbox(project='operator',
render='text') -- the backlog band first, briefs collapsed to one count line each,
your_queue itemized."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.mailbox import OPERATOR_ADDR, send_message


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_desk_render_text_collapses_briefs_and_lists_your_queue(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    p = actions.pool
    await send_message(p, from_agent="agent:a", from_project="desktxtproj",
                       to_project=OPERATOR_ADDR, body="pick a signing strategy",
                       desk_kind="decision")
    await send_message(p, from_agent="agent:b", from_project="desktxtproj",
                       to_project=OPERATOR_ADDR, body="need a key refilled",
                       desk_kind="hands")
    await send_message(p, from_agent="agent:c", from_project="desktxtproj",
                       to_project=OPERATOR_ADDR, body="loop closed, all green",
                       desk_kind="fyi")
    await open_thread(actions, "a duty explicitly placed on the operator",
                      kind="obligation", owner="operator", repo="desktxtproj",
                      source="agent:a")

    ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.inbox(project=OPERATOR_ADDR, peek=True, render="text", ctx=ctx)
    finally:
        srv._pool = saved

    assert set(out.keys()) == {"text"}
    text = out["text"]
    assert "needs decision: 1" in text
    assert "needs hands: 1" in text
    assert "fyi: 1" in text
    assert "pick a signing strategy" not in text  # collapsed, not itemized
    assert "your_queue:" in text
    assert "a duty explicitly placed on the operator" in text
    assert "desktxtproj" in text  # the backlog band, prepended
