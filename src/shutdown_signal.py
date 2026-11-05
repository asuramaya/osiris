"""Make a restart clean: long-lived streams must END when the server is told to stop.

THE PROBLEM (journal, 24 h to 2026-11-05): osiris-console and osiris-mcp logged "Cancel N running
task(s), timeout graceful shutdown exceeded" five times and "ASGI callable returned without
completing response" 71 times. uvicorn's shutdown order is: stop accepting, close idle
connections, WAIT for every open request to finish (up to --timeout-graceful-shutdown, forever
when unset), and only THEN run the app's lifespan shutdown. An SSE stream never finishes by
itself, so a flag set in the lifespan's `finally` (what the console did) is set after the wait it
was meant to end; and the MCP server, run through the SDK's own `mcp.run`, had no flag at all.
Every restart therefore waited out the whole graceful timeout and cancelled the streams mid-body.

THE FIX has two parts, one per service, sharing this module:

* `chain_shutdown_signal` -- the console. Runs inside the lifespan STARTUP (after uvicorn has
  installed its own SIGTERM/SIGINT handlers) and wraps them, so the app's `shutting_down` event is
  set the moment the signal arrives, ahead of uvicorn's connection wait. The console's SSE loops
  already poll that event once per tick.
* `EndStreamsOnShutdown` + `serve_http` -- the MCP server. The SDK's runner hides its uvicorn
  Server, so we run the same Starlette app ourselves, behind an ASGI wrapper that finishes any
  response already streaming (an empty final body, `more_body=False`: a CLEAN end the client
  reconnects from) as soon as the server starts exiting.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import threading
from collections.abc import Callable, MutableMapping
from types import FrameType
from typing import Any

_log = logging.getLogger("osiris.shutdown")

_HANDLED = (signal.SIGTERM, signal.SIGINT)
# A request that has not started its response when shutdown begins is a short call, not a stream:
# let it finish, but only this long.
_PRE_RESPONSE_GRACE_S = 2.0


def chain_shutdown_signal(on_signal: Callable[[], None]) -> Callable[[], None]:
    """Call `on_signal()` (on the event loop) when SIGTERM/SIGINT arrives, then hand the signal on
    to whatever handler was installed before us (uvicorn's). Returns a restore function.

    Must run on the event loop thread; off the main thread (no signals there) it does nothing.
    """
    if threading.current_thread() is not threading.main_thread():
        return lambda: None
    loop = asyncio.get_running_loop()
    ours: dict[int, Any] = {}
    previous: dict[int, Any] = {}

    def make(prev: Any) -> Callable[[int, FrameType | None], None]:
        def handler(sig: int, frame: FrameType | None) -> None:
            with contextlib.suppress(RuntimeError):  # loop already closed
                loop.call_soon_threadsafe(on_signal)
            if callable(prev):
                prev(sig, frame)
        return handler

    for sig in _HANDLED:
        previous[sig] = signal.getsignal(sig)
        ours[sig] = make(previous[sig])
        signal.signal(sig, ours[sig])

    def restore() -> None:
        for sig in _HANDLED:
            # only undo what is still ours; uvicorn restores its own on the way out
            if signal.getsignal(sig) is ours[sig]:
                signal.signal(sig, previous[sig] if previous[sig] is not None else signal.SIG_DFL)

    return restore


class EndStreamsOnShutdown:
    """ASGI wrapper: once `begin_shutdown()` is called, every response that is already streaming
    is finished cleanly instead of waiting for uvicorn to cancel it.

    http only; lifespan and websocket scopes pass straight through.
    """

    def __init__(self, app: Any) -> None:
        self.app = app
        self._stopping = asyncio.Event()

    def begin_shutdown(self) -> None:
        self._stopping.set()

    @property
    def stopping(self) -> bool:
        return self._stopping.is_set()

    async def __call__(self, scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = False
        finished = False

        async def tracked_send(message: MutableMapping[str, Any]) -> None:
            nonlocal started, finished
            if message["type"] == "http.response.start":
                started = True
            elif message["type"] == "http.response.body" and not message.get("more_body", False):
                finished = True
            await send(message)

        app_task = asyncio.ensure_future(self.app(scope, receive, tracked_send))
        stop_task = asyncio.ensure_future(self._stopping.wait())
        try:
            # ended by whichever of the app or the shutdown event comes first
            await asyncio.wait(  # unbounded-wait-ok: the request itself
                {app_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
            if app_task.done():
                app_task.result()
                return
            # shutdown began with this request still open
            if not started:
                await asyncio.wait({app_task}, timeout=_PRE_RESPONSE_GRACE_S)
                if app_task.done():
                    app_task.result()
                    return
            app_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await app_task
            if started and not finished:
                with contextlib.suppress(Exception):
                    await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            stop_task.cancel()
            if not app_task.done():
                app_task.cancel()


def _make_server(app: Any, *, host: str, port: int, log_level: str, graceful_s: int) -> Any:
    import uvicorn

    return uvicorn.Server(uvicorn.Config(
        app, host=host, port=port, log_level=log_level.lower(),
        timeout_graceful_shutdown=graceful_s))


async def serve_http(app: Any, *, host: str, port: int, log_level: str = "info",
                     graceful_s: int = 10) -> None:
    """Run `app` under uvicorn with streams ended on shutdown and a bounded graceful wait.

    Same Config as the SDK's `run_streamable_http_async`, plus `timeout_graceful_shutdown` and
    the EndStreamsOnShutdown wrapper, whose trigger is uvicorn's own `should_exit` flag (polled:
    no signal handling of ours to get wrong).
    """
    wrapped = EndStreamsOnShutdown(app)
    server = _make_server(wrapped, host=host, port=port, log_level=log_level,
                          graceful_s=graceful_s)

    async def watch() -> None:
        # should_exit is a plain bool on uvicorn's side: there is nothing to await
        while not server.should_exit:  # noqa: ASYNC110
            await asyncio.sleep(0.1)
        wrapped.begin_shutdown()

    watcher = asyncio.create_task(watch())
    try:
        await server.serve()
    finally:
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher
