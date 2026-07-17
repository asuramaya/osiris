"""The PTY broker (`src/manager/pty_broker.py`) — tmux-with-a-graph's "tmux" half, tested
against REAL PTYs and REAL children throughout (never a fake subprocess: the whole point of
this module is that a raw fd behaves like a terminal, which a fake cannot prove). No Postgres,
no Redis — this module doesn't touch the graph at all.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
from collections.abc import Callable
from pathlib import Path

import pytest
from src.manager.pty_broker import (
    _MAX_CHUNKS_PER_WAKEUP,
    DEFAULT_ATTACH_QUEUE_CHUNKS,
    FRAME_TYPE_EXITED,
    FRAME_TYPE_INPUT,
    FRAME_TYPE_OUTPUT,
    FRAME_TYPE_RESIZE,
    PtyBroker,
    PtySession,
    SessionExistsError,
    _RingBuffer,
    encode_hello,
    pack_frame,
    read_frame,
    read_hello,
    serve_session,
)

requires_pty = pytest.mark.skipif(
    not hasattr(os, "openpty"), reason="os.openpty() is unavailable on this platform")


async def _collect_until(
    queue: asyncio.Queue[bytes | None], predicate: Callable[[bytes], bool],
) -> bytes:
    """Reads from an attach queue, accumulating, until `predicate(accumulated)` holds or the
    exited sentinel arrives first. Bounded by the CALLER's `asyncio.timeout` — a timeout means
    the expected output never showed up, which is a test failure, not a thing to swallow."""
    buf = bytearray()
    while not predicate(bytes(buf)):
        item = await queue.get()
        if item is None:
            return bytes(buf)
        buf.extend(item)
    return bytes(buf)


async def _await_in_ring(session: PtySession, needle: bytes) -> None:
    """Attaches, waits for `needle` to appear in the live stream (or is already in the replay),
    detaches. After this returns, `session.attach()`'s replay is guaranteed to contain `needle`
    too — ring writes happen before fan-out inside `_on_readable`, never after."""
    replay, queue = session.attach()
    try:
        if needle not in replay:
            async with asyncio.timeout(2.0):
                await _collect_until(queue, lambda buf: needle in buf)
    finally:
        session.detach(queue)


# --- _RingBuffer: pure, no pty involved -----------------------------------------------------


def test_ring_buffer_holds_everything_under_the_cap() -> None:
    ring = _RingBuffer(10)
    ring.append(b"0123456789")
    assert ring.snapshot() == b"0123456789"
    assert len(ring) == 10


def test_ring_buffer_drops_oldest_bytes_first_once_over_cap() -> None:
    ring = _RingBuffer(10)
    ring.append(b"0123456789")
    ring.append(b"ABCDE")
    assert len(ring) == 10
    assert ring.snapshot() == b"56789ABCDE"


def test_ring_buffer_trims_a_single_chunk_across_the_cap_boundary() -> None:
    ring = _RingBuffer(5)
    ring.append(b"abc")
    ring.append(b"defgh")  # 8 bytes total, over the 5-byte cap
    assert ring.snapshot() == b"defgh"  # "abc" fully dropped; "defgh" alone already fills the cap


def test_ring_buffer_rejects_a_non_positive_cap() -> None:
    with pytest.raises(ValueError):
        _RingBuffer(0)


# --- the framed wire protocol: pure, no socket or pty involved ------------------------------


async def test_pack_and_read_frame_round_trips() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(pack_frame(FRAME_TYPE_OUTPUT, b"hello"))
    reader.feed_eof()
    assert await read_frame(reader) == (FRAME_TYPE_OUTPUT, b"hello")


async def test_read_frame_returns_none_on_clean_disconnect_before_any_frame() -> None:
    reader = asyncio.StreamReader()
    reader.feed_eof()
    assert await read_frame(reader) is None


async def test_read_frame_returns_none_on_disconnect_mid_frame() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(pack_frame(FRAME_TYPE_OUTPUT, b"hello")[:3])  # header only, truncated
    reader.feed_eof()
    assert await read_frame(reader) is None


async def test_hello_encode_and_read_round_trips() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(encode_hello("seat-1", 24, 80))
    reader.feed_eof()
    assert await read_hello(reader) == {"name": "seat-1", "rows": 24, "cols": 80}


async def test_read_hello_returns_none_on_disconnect_before_a_line() -> None:
    reader = asyncio.StreamReader()
    reader.feed_eof()
    assert await read_hello(reader) is None


# --- PtySession: real pty, real children ----------------------------------------------------


@requires_pty
async def test_replay_contains_output_produced_before_attaching() -> None:
    session = await PtySession.spawn(["sh", "-c", "echo hello; cat"])
    try:
        await _await_in_ring(session, b"hello")
        replay, queue = session.attach()
        try:
            assert b"hello" in replay
        finally:
            session.detach(queue)
    finally:
        await session.close()


@requires_pty
async def test_write_round_trips_to_the_attach_queue() -> None:
    session = await PtySession.spawn(["sh", "-c", "echo hello; cat"])
    try:
        await _await_in_ring(session, b"hello")
        replay, queue = session.attach()
        try:
            session.write(b"x\n")
            async with asyncio.timeout(2.0):
                data = await _collect_until(queue, lambda buf: b"x" in buf)
            assert b"x" in data
        finally:
            session.detach(queue)
    finally:
        await session.close()


@requires_pty
async def test_second_attach_gets_full_replay_while_first_stays_live() -> None:
    session = await PtySession.spawn(["sh", "-c", "echo hello; cat"])
    try:
        await _await_in_ring(session, b"hello")
        replay1, queue1 = session.attach()
        replay2, queue2 = session.attach()
        try:
            assert b"hello" in replay1
            assert b"hello" in replay2
            assert session.attach_count == 2  # two faces, one seat

            session.write(b"marker\n")
            async with asyncio.timeout(2.0):
                data1 = await _collect_until(queue1, lambda buf: b"marker" in buf)
                data2 = await _collect_until(queue2, lambda buf: b"marker" in buf)
            assert b"marker" in data1
            assert b"marker" in data2  # the SECOND attach never missed the first's traffic
        finally:
            session.detach(queue1)
            session.detach(queue2)
    finally:
        await session.close()


@requires_pty
async def test_poke_types_one_bracketed_message_and_submits() -> None:
    """THE POKE (the wake law): a multi-line prompt lands as ONE bracketed paste
    (ESC[200~ … ESC[201~) followed by CR — the tty's echo puts the typed bytes in the
    ring, so the poke is witnessed by the same scrollback every face replays."""
    session = await PtySession.spawn(["sh", "-c", "echo ready; cat"])
    try:
        await _await_in_ring(session, b"ready")
        session.poke("new mail waiting\ninbox() and settle it")
        await _await_in_ring(session, b"settle it")
        replay, queue = session.attach()
        try:
            # the echo renders ESC as caret notation (^[), so match the marker's tail —
            # present either way: one paste envelope, not N separately-submitted lines
            assert b"[200~" in replay and b"[201~" in replay
            assert b"new mail waiting" in replay
        finally:
            session.detach(queue)
    finally:
        await session.close()


@requires_pty
async def test_idle_seconds_is_reset_by_output() -> None:
    """The busy-signal for the poke gate: output (streaming, or the echo of typing) resets
    the idle clock; silence grows it. Coarse bounds only — this is a clock, not a stopwatch."""
    session = await PtySession.spawn(["sh", "-c", "echo hello; cat"])
    try:
        await _await_in_ring(session, b"hello")
        await asyncio.sleep(0.6)
        assert session.idle_seconds >= 0.5      # silence accrued
        session.write(b"tick\n")
        await _await_in_ring(session, b"tick")  # the echo is output — the clock resets
        assert session.idle_seconds < 0.5
    finally:
        await session.close()


@requires_pty
async def test_detach_leaves_the_child_running() -> None:
    session = await PtySession.spawn(["sh", "-c", "echo hello; cat"])
    try:
        await _await_in_ring(session, b"hello")
        replay, queue = session.attach()
        session.detach(queue)
        assert session.attach_count == 0
        await asyncio.sleep(0.1)
        assert session.returncode is None  # detach never kills the child — that's the point
    finally:
        await session.close()


@requires_pty
async def test_resize_changes_the_pty_size_observed_inside_the_session() -> None:
    # a genuinely interactive shell — `sh -c "stty size; cat"` would only run `stty` ONCE at
    # startup and become plain `cat` forever after, so a second "stty size\n" would just get
    # echoed back as literal text instead of executed.
    session = await PtySession.spawn(["sh"], rows=24, cols=80)
    try:
        replay, queue = session.attach()
        try:
            session.write(b"stty size\n")
            async with asyncio.timeout(2.0):
                initial = await _collect_until(queue, lambda buf: b"24 80" in buf)
            assert b"24 80" in initial

            session.resize(50, 120)
            assert (session.rows, session.cols) == (50, 120)
            session.write(b"stty size\n")
            async with asyncio.timeout(2.0):
                data = await _collect_until(queue, lambda buf: b"50 120" in buf)
            assert b"50 120" in data
        finally:
            session.detach(queue)
    finally:
        await session.close()


@requires_pty
async def test_child_exit_is_observed_and_the_ring_stays_readable() -> None:
    session = await PtySession.spawn(["sh", "-c", "echo bye"])
    try:
        await asyncio.wait_for(session.exited.wait(), timeout=2.0)
        assert session.returncode == 0

        # a face attaching to a corpse still sees the last screen...
        replay, queue = session.attach()
        try:
            assert b"bye" in replay
            # ...plus the exited marker, immediately (no need to have been watching earlier)
            sentinel = await asyncio.wait_for(queue.get(), timeout=1.0)
            assert sentinel is None
        finally:
            session.detach(queue)
    finally:
        await session.close()


@requires_pty
async def test_close_reaps_a_long_running_child() -> None:
    session = await PtySession.spawn(["sh", "-c", "while true; do sleep 0.05; done"])
    await session.close(grace=1.0)
    assert session.returncode is not None  # SIGHUP terminates a plain sh loop directly
    await session.close(grace=1.0)  # idempotent: a second close() must not raise or re-signal


@requires_pty
async def test_close_escalates_to_sigkill_when_the_child_ignores_sighup() -> None:
    # The child echoes a marker AFTER installing its HUP trap; the test waits for it before
    # signalling. Without this sync, close()'s SIGHUP can race ahead of the shell's `trap`
    # and hit the DEFAULT action — the child dies with -SIGHUP (-1), not the -SIGKILL this
    # test is about. A load-sensitive flake (reproduced ~11% under full-file load,
    # 2026-07-15): the escalation logic was never wrong, the test was racing child readiness.
    session = await PtySession.spawn(
        ["sh", "-c", "trap '' HUP; echo __trapped__; while true; do sleep 0.05; done"])
    _, queue = session.attach()
    async with asyncio.timeout(10.0):
        buf = b""
        while b"__trapped__" not in buf:
            chunk = await queue.get()
            if chunk is None:
                break
            buf += chunk
    await session.close(grace=0.3)
    assert session.returncode == -signal.SIGKILL


# --- PtyBroker: the named registry -----------------------------------------------------------


@requires_pty
async def test_broker_spawn_get_list_close_and_name_collisions() -> None:
    broker = PtyBroker()
    session = await broker.spawn("seat-1", ["sh", "-c", "cat"])
    try:
        assert broker.get("seat-1") is session
        assert broker.get("nonexistent") is None

        infos = broker.list()
        assert len(infos) == 1
        assert infos[0].name == "seat-1"
        assert infos[0].alive is True
        assert infos[0].attach_count == 0

        with pytest.raises(SessionExistsError):
            await broker.spawn("seat-1", ["sh", "-c", "cat"])  # refuses loudly, never silently

        replay, queue = session.attach()
        assert broker.list()[0].attach_count == 1
        session.detach(queue)
    finally:
        await broker.close("seat-1")
    assert broker.get("seat-1") is None
    assert session.returncode is not None  # broker.close() actually reaped it

    await broker.close("seat-1")  # unknown name: idempotent no-op, same as bodies.dissolve


# --- backpressure: bounded queues + force-detach (doctrine 3 — nothing in the sacred proc ---
# --- may grow without bound; a stalled face is disconnected, never accumulated)            ---


@requires_pty
async def test_attach_queues_are_bounded() -> None:
    """The regression guard the whole backpressure discipline hangs on: `asyncio.Queue(0)` is
    UNBOUNDED, so maxsize must be provably positive — asserted on the actual queue object a
    real attach() hands out, not on a constant a refactor could silently disconnect from it."""
    session = await PtySession.spawn(["sh", "-c", "cat"])
    try:
        replay, queue = session.attach()
        assert queue.maxsize == DEFAULT_ATTACH_QUEUE_CHUNKS
        assert queue.maxsize > 0
        session.detach(queue)
    finally:
        await session.close()

    with pytest.raises(ValueError):  # an unbounded queue must be unconstructable, not a default
        await PtySession.spawn(["sh"], queue_maxsize=0)


@requires_pty
async def test_flooding_child_force_detaches_only_the_stalled_attachment() -> None:
    """The daemon-OOM scenario, exactly: a chatty child (`yes`), one attachment that never
    drains. The stalled queue must cap at maxsize and get force-detached ALONE — child alive,
    sibling attachment streaming, and the ring replay waiting for the casualty's reattach.
    queue_maxsize deliberately exceeds `_MAX_CHUNKS_PER_WAKEUP`: the per-wakeup drain budget is
    what guarantees the well-behaved sibling gets scheduled between bursts and never overflows
    alongside the guilty one — the margin below is that invariant, asserted."""
    session = await PtySession.spawn(["sh", "-c", "yes"], queue_maxsize=32)
    try:
        assert 32 > _MAX_CHUNKS_PER_WAKEUP  # one burst can never fill a healthy queue outright
        _, stalled = session.attach()  # never drained — the slow consumer
        _, live = session.attach()  # well-behaved sibling
        assert stalled.maxsize == 32
        assert session.attach_count == 2

        # event-driven wait: the flood always yields to the live queue, and each arrival is a
        # chance for the fan-out to have tripped the stalled queue's bound
        async with asyncio.timeout(5.0):
            while session.attach_count == 2:
                item = await live.get()
                assert item is not None  # the live attachment never sees an end-marker here

        # the stalled attachment, ALONE, was the casualty...
        assert session.attach_count == 1
        assert stalled.qsize() <= stalled.maxsize  # never grew past the bound
        # ...its undeliverable backlog discarded, terminated with the end-marker
        assert stalled.get_nowait() is None
        # the child and the well-behaved sibling are untouched
        assert session.returncode is None
        async with asyncio.timeout(2.0):
            more = await live.get()
        assert more is not None and b"y" in more

        # reattach after force-detach: the ring replay is the recovery path
        replay, fresh = session.attach()
        assert b"y" in replay
        session.detach(fresh)
        session.detach(live)
    finally:
        await session.close()


@requires_pty
async def test_serve_session_closes_the_socket_of_a_force_detached_client(
    tmp_path: Path,
) -> None:
    """The full recovery loop over the wire: a client that connects and never reads parks the
    server's pump in drain(); the session-side queue fills and force-detaches it; when the
    client finally reads again it gets the buffered frames, then a clean EOF (no 'X' — the
    child is alive); a reconnect gets the ring replay. Force-detach is detected via the
    broker's own log line (event-driven — the warning IS the observable), which also proves
    the honest-log requirement."""
    detached = asyncio.Event()

    class _DetachSignal(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            detached.set()  # emitted from the loop thread (fan-out runs in a reader callback)

    handler = _DetachSignal()
    logging.getLogger("osiris.pty_broker").addHandler(handler)

    session = await PtySession.spawn(["sh", "-c", "yes"], queue_maxsize=8)
    socket_path = str(tmp_path / "flood.sock")
    server = await serve_session(session, socket_path)
    try:
        reader, writer = await asyncio.open_unix_connection(socket_path)
        try:
            writer.write(encode_hello("seat-flood", 24, 80))
            await writer.drain()
            # deliberately read NOTHING: kernel buffers fill, the pump parks in drain(),
            # the queue caps out, the session force-detaches this attachment.
            # (This log-based observable went dark for a WEEK because alembic's fileConfig
            # disabled every existing logger at the test session's first migration — thread
            # bbca30a9; cured at the source in alembic/env.py, disable_existing_loggers=False.)
            async with asyncio.timeout(10.0):
                await detached.wait()
            assert session.attach_count == 0
            assert session.returncode is None  # the child never paid for its client's stall

            # the client resumes reading: buffered 'O' frames, then clean EOF — never an 'X'
            # (the child is alive; the EOF is the force-detach notification)
            async with asyncio.timeout(10.0):
                while True:
                    frame = await read_frame(reader)
                    if frame is None:
                        break
                    assert frame[0] == FRAME_TYPE_OUTPUT
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

        # recovery: reconnect, and the ring replay catches the face up
        reader2, writer2 = await asyncio.open_unix_connection(socket_path)
        try:
            writer2.write(encode_hello("seat-flood", 24, 80))
            await writer2.drain()
            async with asyncio.timeout(5.0):
                await _read_output_until(reader2, b"y")
        finally:
            writer2.close()
            with contextlib.suppress(OSError):
                await writer2.wait_closed()
    finally:
        logging.getLogger("osiris.pty_broker").removeHandler(handler)
        server.close()
        await server.wait_closed()
        await session.close()


# --- end-to-end: serve_session + the wire protocol, driven directly over asyncio streams -----


async def _read_output_until(reader: asyncio.StreamReader, needle: bytes) -> None:
    """Reads framed 'O' output from the socket until `needle` shows up — an event-driven wait
    (each iteration blocks on the next frame, never on a sleep), bounded by the caller's own
    `asyncio.timeout`."""
    buf = bytearray()
    while needle not in buf:
        frame = await read_frame(reader)
        assert frame is not None
        frame_type, payload = frame
        assert frame_type == FRAME_TYPE_OUTPUT
        buf.extend(payload)


@requires_pty
async def test_serve_session_end_to_end_over_a_unix_socket(tmp_path: Path) -> None:
    # a real interactive shell (not `sh -c ...; cat`) so it keeps taking commands — this test
    # drives BOTH an echo and a resize-sensitive `stty size` through the wire protocol.
    session = await PtySession.spawn(["sh"], rows=30, cols=100)
    socket_path = str(tmp_path / "seat.sock")
    server = await serve_session(session, socket_path)
    try:
        reader, writer = await asyncio.open_unix_connection(socket_path)
        try:
            writer.write(encode_hello("seat-1", 30, 100))
            await writer.drain()
            assert (session.rows, session.cols) == (30, 100)  # hello negotiated the size

            # input, framed, round-tripping through the socket
            writer.write(pack_frame(FRAME_TYPE_INPUT, b"echo hello-marker\n"))
            await writer.drain()
            async with asyncio.timeout(2.0):
                await _read_output_until(reader, b"hello-marker")

            # resize, as an OOB frame, through the socket — then prove the CHILD saw it too
            resize_payload = json.dumps({"rows": 40, "cols": 110}).encode()
            writer.write(pack_frame(FRAME_TYPE_RESIZE, resize_payload))
            writer.write(pack_frame(FRAME_TYPE_INPUT, b"stty size\n"))
            await writer.drain()
            async with asyncio.timeout(2.0):
                await _read_output_until(reader, b"40 110")
            assert (session.rows, session.cols) == (40, 110)
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()
        await session.close()


@requires_pty
async def test_serve_session_sends_exited_frame_after_the_child_dies(tmp_path: Path) -> None:
    session = await PtySession.spawn(["sh", "-c", "echo bye"])
    socket_path = str(tmp_path / "seat2.sock")
    server = await serve_session(session, socket_path)
    try:
        reader, writer = await asyncio.open_unix_connection(socket_path)
        try:
            writer.write(encode_hello("seat-2", 24, 80))
            await writer.drain()

            saw_exited = False
            for _ in range(50):  # bounded: a hung server would otherwise loop forever
                frame = await asyncio.wait_for(read_frame(reader), timeout=2.0)
                assert frame is not None
                frame_type, payload = frame
                if frame_type == FRAME_TYPE_EXITED:
                    info = json.loads(payload.decode())
                    assert info["returncode"] == 0
                    saw_exited = True
                    break
            assert saw_exited
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()
        await session.close()
