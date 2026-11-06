"""A restart must not wait out the graceful timeout on open SSE streams (src/shutdown_signal.py).

Journal, 24 h to 2026-11-05: 5x "Cancel N running task(s), timeout graceful shutdown exceeded",
71x "ASGI callable returned without completing response" across osiris-console and osiris-mcp.
"""
from __future__ import annotations

import asyncio
import signal
import socket
import time
from typing import Any

import httpx
import pytest
from src import shutdown_signal
from src.shutdown_signal import EndStreamsOnShutdown, chain_shutdown_signal, serve_http


async def _sse_app(scope: Any, receive: Any, send: Any) -> None:
    """An endless SSE stream on /stream; a plain short response elsewhere."""
    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            else:
                await send({"type": "lifespan.shutdown.complete"})
                return
    if scope["path"] == "/stream":
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"text/event-stream")]})
        while True:
            await send({"type": "http.response.body", "body": b": keep-alive\n\n",
                        "more_body": True})
            await asyncio.sleep(0.05)
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


async def test_a_streaming_response_is_finished_cleanly_on_shutdown() -> None:
    wrapped = EndStreamsOnShutdown(_sse_app)
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    async def receive() -> dict[str, Any]:
        await asyncio.sleep(3600)
        return {}

    task = asyncio.create_task(wrapped({"type": "http", "path": "/stream"}, receive, send))
    await asyncio.sleep(0.2)
    assert not task.done()
    wrapped.begin_shutdown()
    await asyncio.wait_for(task, 2.0)
    assert sent[0]["type"] == "http.response.start"
    assert sent[-1] == {"type": "http.response.body", "body": b"", "more_body": False}


async def test_a_short_request_in_flight_at_shutdown_still_completes() -> None:
    async def slowish(scope: Any, receive: Any, send: Any) -> None:
        await asyncio.sleep(0.2)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"done"})

    wrapped = EndStreamsOnShutdown(slowish)
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    task = asyncio.create_task(wrapped({"type": "http"}, lambda: None, send))
    await asyncio.sleep(0.05)
    wrapped.begin_shutdown()
    await asyncio.wait_for(task, 2.0)
    assert [m.get("body") for m in sent[1:]] == [b"done"]


async def test_the_signal_chain_calls_us_then_the_previous_handler() -> None:
    seen: list[str] = []
    loop_event = asyncio.Event()
    original = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda sig, frame: seen.append("previous"))
    try:
        restore = chain_shutdown_signal(lambda: (seen.append("ours"), loop_event.set()))
        signal.raise_signal(signal.SIGTERM)
        await asyncio.wait_for(loop_event.wait(), 2.0)
        assert seen == ["previous", "ours"] or seen == ["ours", "previous"]
        assert set(seen) == {"previous", "ours"}
        restore()
        assert callable(signal.getsignal(signal.SIGTERM))
        seen.clear()
        signal.raise_signal(signal.SIGTERM)
        assert seen == ["previous"]  # restored: ours no longer fires
    finally:
        signal.signal(signal.SIGTERM, original)


def _free_port() -> int:
    with socket.socket() as s:  # unbounded-wait-ok: bind to port 0 only, never connects
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def test_a_real_uvicorn_restart_does_not_wait_on_an_open_stream(
        monkeypatch: pytest.MonkeyPatch) -> None:
    servers: list[Any] = []
    real_make = shutdown_signal._make_server

    def capture(*args: Any, **kwargs: Any) -> Any:
        server = real_make(*args, **kwargs)
        servers.append(server)
        return server

    monkeypatch.setattr(shutdown_signal, "_make_server", capture)
    port = _free_port()
    # graceful_s is the ceiling a stream WOULD have burned; the stream must end well inside it
    serving = asyncio.create_task(serve_http(_sse_app, host="127.0.0.1", port=port,
                                             log_level="warning", graceful_s=30))
    for _ in range(100):
        if servers and servers[0].started:
            break
        await asyncio.sleep(0.05)
    assert servers and servers[0].started

    async def hold_stream() -> int:
        chunks = 0
        async with httpx.AsyncClient(timeout=30) as client, client.stream(
                "GET", f"http://127.0.0.1:{port}/stream") as resp:
            async for _ in resp.aiter_bytes():
                chunks += 1
        return chunks

    reader = asyncio.create_task(hold_stream())
    await asyncio.sleep(0.4)
    assert not reader.done()
    t0 = time.monotonic()
    servers[0].should_exit = True
    chunks = await asyncio.wait_for(reader, 10.0)
    await asyncio.wait_for(serving, 10.0)
    assert chunks >= 1
    assert time.monotonic() - t0 < 5.0  # not the 30 s graceful ceiling
