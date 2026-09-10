"""THE INBOX'S ROUTES (task #71) — GET / now just redirects to /ui (the operator's own
front-door consolidation, 2026-09-10; the standalone shell ruling 0b3dd431 minted is
dead). GET /stream (SSE) and POST /inbox/{id}/{action} are left as-is here, out of this
redirect-only fix's scope. Mounted into the existing :8011 process via create_app()'s
own include_router() (a separate cutover commit, per Thoth's sequencing, msg 1818) —
this module never starts its own uvicorn process; :8011 stays one process, one port.

Every route here is a THIN adapter: inbox.py builds the Block tree, render.py turns it
into HTML, and POST actions dispatch through the SAME closed ACTION_VERBS registry /act
already uses (src/api/actions.py) — never a second write path."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import asyncpg
from datastar_py.fastapi import DatastarResponse
from datastar_py.fastapi import ServerSentEventGenerator as SSE
from datastar_py.sse import DatastarEvent
from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse

from src.api.app import get_pool
from src.api.inbox.inbox import build_inbox
from src.api.inbox.render import render_block

router = APIRouter()

_STREAM_INTERVAL_SECS = 5


@router.get("/")
async def inbox_shell() -> RedirectResponse:
    """:8011's front door is /ui (the consolidation the operator named live, 2026-09-10) —
    the standalone Inbox shell this route used to render (ruling 0b3dd431) is dead; this
    redirect is the fix, not a rebuild, per Thoth's own framing of the same ask. /stream
    and the POST /inbox/{id}/{action} actions stay put, untouched by this route alone."""
    return RedirectResponse(url="/ui/")


async def _stream_events(request: Request) -> AsyncIterator[DatastarEvent]:
    pool = request.app.state.pool
    while not await request.is_disconnected():
        inbox_list = await build_inbox(pool)
        yield SSE.patch_elements(render_block(inbox_list), selector="#inbox-list")
        await asyncio.sleep(_STREAM_INTERVAL_SECS)


@router.get("/stream")
async def inbox_stream(request: Request) -> DatastarResponse:
    """One SSE connection per page load (shell.html.j2's data-on-load) — re-renders the
    inbox-list region on an interval and patches it in place via Datastar's idiomorph-class
    morph (default patch_elements mode): stable ids, no layout shift, focus/scroll
    preserved. Stops cleanly the moment the browser disconnects.

    Built by hand (DatastarResponse(...) directly) rather than the @datastar_response
    decorator: that decorator's functools.wraps leaves this FastAPI version's own
    async-generator route auto-detection finding the WRAPPED generator underneath,
    'async for'-ing over a bare coroutine instead of calling the wrapper — confirmed live,
    not theorized (TypeError: 'async for' requires an object with __aiter__ method, got
    coroutine). Constructing the response directly sidesteps the interaction entirely."""
    return DatastarResponse(_stream_events(request))


@router.post("/inbox/{item_id}/{action}")
async def inbox_action(
    item_id: str, action: str, pool: asyncpg.Pool = Depends(get_pool),
) -> dict[str, object]:
    """Dispatch through the SAME closed registry /act already uses (src/api/actions.py) —
    never a bespoke write path. The two live-desk actions this slice's items ever carry
    (resolve_thread, settle) take different arg shapes at that registry's own boundary;
    this is the one place that reconciles item_id -> the right shape, nothing more."""
    from src.api.actions import ACTION_VERBS

    verb = ACTION_VERBS.get(action)
    if verb is None:
        return {"error": f"unknown action {action!r} — must be one of {sorted(ACTION_VERBS)}"}
    args = {"ids": item_id} if action == "settle" else {"ref": item_id}
    return await verb(pool, args)
