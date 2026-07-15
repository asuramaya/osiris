"""THE PTY BROKER — tmux-with-a-graph, minus the graph (SPEC-agent-manager.md §2, §4.5).

Phase 1's daemon (`osiris-manager`, not yet built — this module depends on NOTHING under
`src/manager/` besides the stdlib, so wave 2 can import it whole) will hold one `PtyBroker` for
the fleet: every managed seat's terminal is a real PTY, spawned once, kept alive across however
many faces attach and detach. Detach/reattach are tmux's proven semantics (§2's "tmux-with-a-
graph"); this module supplies the "tmux" half — a later graph join is wave 2's job, not this
module's.

THE TWO LANES (§2): a PTY is the universal, dumb, faithful lane — full VT bytes, zero parsing,
zero assumptions about the tenant (`claude`, `vim`, a bare shell — all the same to this module).
The graph lane (mount/decisions/mail) is a SEPARATE concern this module never touches.

HONESTY NOTE — READ BEFORE TRUSTING THE REPLAY: `PtySession` keeps a RING BUFFER of raw output
BYTES, not a parsed screen. Replaying it on attach reproduces SCROLLBACK — everything the
program printed, verbatim, in order — which is enough for line-oriented output and enough for a
REAL terminal emulator client (xterm.js, a VT100 widget, `attach.py`'s own passthrough) to
re-render correctly, because such a client already reconstructs screen state from a raw byte
stream by design; a full-screen TUI (vim, htop, claude's own ink-style UI) also repaints itself
correctly on the NEXT redraw once reattached, same as a real terminal after a resize. What this
buffer does NOT do is reconstruct "the current screen" as a grid of cells the way `tmux attach`
or a `pyte`-style parser would — if the last thing on screen was drawn via cursor-positioning
escape sequences over OLD content that later scrolled out of the ring, replaying the ring alone
won't hand you a clean redrawn frame, just the byte history that produced one. The upgrade path,
should a client ever need a synthesized "current frame" instead of raw scrollback, is a
screen-state parser (a pyte-shaped VT100 grid emulator) sitting between the ring and the
replay — named here, deliberately NOT built: no new dependency, no speculative machinery ahead
of a client that needs it (doctrine 4, `5cd5b7b6` — no band-aids, build what's asked).

WHY `add_reader`/`add_writer`, not `connect_read_pipe` (the seam this module had to choose,
`bodies.py`'s `ProcessRunner` being the closest existing precedent for "an injectable seam
around an OS boundary"): `connect_read_pipe`/`connect_write_pipe` want a `Protocol` object and
build a whole transport around it — the right shape for wrapping a subprocess's own stdio pipes,
but this module bridges a SINGLE raw fd (the PTY master) that is simultaneously read from and
written to, wants raw bytes with no framing, and needs the read side torn down independently of
the write side on hangup. `loop.add_reader(fd, callback)` / `loop.add_writer(fd, callback)` are
the plain, stdlib-documented way to hand the selector a bare fd and a synchronous callback —
nothing to translate at the boundary, no Protocol ceremony for a job this small.

WHY a real controlling terminal (not just an fd): a child spawned with its stdio pointed at the
PTY's slave end, but never made the session leader of a session that has claimed that slave as
its controlling terminal, never gets `SIGWINCH` on resize and never gets normal job-control
signals — indistinguishable, to that child, from being run under a dumb pipe. Linux hands a
process a controlling terminal automatically only when a session leader OPENS a tty by path; a
child that merely inherits an already-open fd (which is what happens when the fds are dup'd
across an `os.openpty()` + subprocess spawn, as here) does NOT get one for free. `spawn()` fixes
this the same way `pexpect`/`ptyprocess` do: `start_new_session=True` (equivalent to `setsid()`
in the child, before anything else runs) plus a `preexec_fn` that claims the terminal explicitly
via `ioctl(0, TIOCSCTTY, 0)` once fd 0 is the PTY slave. Verified empirically against this
module's actual target (Linux, CPython's `subprocess`/`asyncio` internals) before being trusted:
`tty` run inside the child reports the slave's path, and a `TIOCSWINSZ` on the master genuinely
delivers `SIGWINCH` to the child's `trap`, both only once this dance is done.

PLATFORM: Linux only, deliberately (`TIOCSCTTY`/`TIOCSWINSZ` values, `/proc`-free but PTY-
semantics-dependent — the EIO-on-hangup behavior this module relies on for detecting a dead
child is a Linux/BSD pty convention, not a POSIX guarantee). The rest of Osiris already assumes
Linux (systemd, cgroup v2); this module inherits that assumption rather than fighting it.
"""
from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import logging
import os
import signal
import struct
import termios
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("osiris.pty_broker")

# --- tunables ---------------------------------------------------------------------------

DEFAULT_TERM = "xterm-256color"
DEFAULT_RING_SIZE = 256 * 1024  # 256 KiB of scrollback replayed to a reattaching face
_READ_CHUNK = 65536  # one read() worth — generous; a PTY rarely buffers more per read

# The synchronous drain budget, in chunks per reader wakeup: 16 * _READ_CHUNK = 1 MiB, then the
# callback yields back to the event loop (the fd stays readable; level-triggered add_reader just
# re-fires it next iteration). Two reasons, both doctrine 3 (`2ceb7ba0`): a flooding child
# (`yes`, a runaway build log) must not wedge the SACRED PROC's event loop inside one callback,
# and a single burst's fan-out must stay smaller than an attachment queue's bound so a
# well-behaved consumer always gets scheduled to drain between bursts — without the cap, one
# burst could overflow a HEALTHY attachment's queue before its reader ever ran, and the
# force-detach below would punish the innocent.
_MAX_CHUNKS_PER_WAKEUP = 16

# Per-attachment queue bound, in CHUNKS. Each queued item is one os.read() result, so the hard
# per-attachment ceiling is DEFAULT_ATTACH_QUEUE_CHUNKS * _READ_CHUNK = 256 * 64 KiB = 16 MiB —
# and that worst case needs every chunk read at the full 64 KiB, which a PTY essentially never
# delivers (the line discipline hands out ~4 KiB reads in practice, putting a full queue nearer
# 1 MiB). The bound exists for doctrine 3 (`2ceb7ba0`): this broker lives inside the SACRED
# PROC, and a consumer that stops reading must never become an unbounded sink that OOMs the
# daemon — see PtySession's class docstring for what happens when the bound is hit.
DEFAULT_ATTACH_QUEUE_CHUNKS = 256


def _claim_controlling_tty() -> None:
    """The child's `preexec_fn` (see the module docstring's "WHY a real controlling terminal").
    Runs AFTER `subprocess`'s own C-level fd setup (stdin/stdout/stderr are already dup2'd onto
    0/1/2 by the time any `preexec_fn` executes) and after `start_new_session`'s `setsid()` —
    so fd 0 is reliably the PTY slave, and this process is reliably a session leader with no
    controlling terminal yet. `TIOCSCTTY` claims it."""
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)


class _RingBuffer:
    """The REPLAY-ON-ATTACH state (see the module's HONESTY NOTE): a byte log capped at
    `max_bytes`, oldest bytes dropped first. Chunks (exactly as `os.read()` produced them) are
    kept in a deque rather than one giant `bytearray`, so trimming an overflow is a handful of
    `popleft()`s off the front instead of an O(n) memmove on every single read — the difference
    matters once a chatty child has pushed megabytes through a 256 KiB ring over a long
    session."""

    __slots__ = ("_max_bytes", "_chunks", "_size")

    def __init__(self, max_bytes: int) -> None:
        if max_bytes <= 0:
            raise ValueError(f"ring_size must be positive, got {max_bytes}")
        self._max_bytes = max_bytes
        self._chunks: deque[bytes] = deque()
        self._size = 0

    def append(self, data: bytes) -> None:
        if not data:
            return
        self._chunks.append(data)
        self._size += len(data)
        while self._size > self._max_bytes:
            oldest = self._chunks[0]
            overflow = self._size - self._max_bytes
            if overflow >= len(oldest):  # the whole chunk is stale — drop it entirely
                self._chunks.popleft()
                self._size -= len(oldest)
            else:  # only its head is stale — trim in place, keep the tail
                self._chunks[0] = oldest[overflow:]
                self._size -= overflow

    def snapshot(self) -> bytes:
        """The replay: everything currently held, oldest first, concatenated verbatim."""
        return b"".join(self._chunks)

    def __len__(self) -> int:
        return self._size


class PtySession:
    """One child, one real PTY, one ring of scrollback — the unit `PtyBroker` names and a face
    attaches to. THE SESSION OUTLIVES ATTACHMENTS: `detach()` never touches the child, and even
    a child that has already exited keeps its ring readable (a face attaching to a corpse sees
    the last screen plus the exited marker) until `close()` is called explicitly. Construct via
    `PtySession.spawn(...)`, never `PtySession(...)` directly — spawning needs a running event
    loop (for `add_reader`/the exit-watch task) and an already-open PTY pair, both of which the
    classmethod sets up before handing back a usable object.

    BACKPRESSURE — tmux semantics all the way down (doctrine 3, `2ceb7ba0`: this object lives
    inside the sacred proc, so NOTHING here may grow without bound): every attachment queue is
    bounded at `queue_maxsize` chunks. A consumer that stops draining — a face that hung, a
    dead peer whose socket never flushes — fills its queue and gets FORCE-DETACHED on the
    overflowing chunk: dropped from the fan-out, its queue cleared and terminated with the
    `None` end-marker, one honest log line. The session, the ring, and every OTHER attachment
    are untouched, and the ring IS the casualty's recovery path: reattach, get the replay,
    catch up — exactly what a killed `tmux attach` does.
    """

    def __init__(
        self, *, master_fd: int, proc: asyncio.subprocess.Process, rows: int, cols: int,
        ring_size: int, queue_maxsize: int,
    ) -> None:
        if queue_maxsize <= 0:  # 0 means UNBOUNDED to asyncio.Queue — the exact regression
            raise ValueError(f"queue_maxsize must be positive, got {queue_maxsize}")
        self._master_fd = master_fd
        self._proc = proc
        self._rows = rows
        self._cols = cols
        self._ring = _RingBuffer(ring_size)
        self._queue_maxsize = queue_maxsize
        self._attachments: set[asyncio.Queue[bytes | None]] = set()
        self._exited = asyncio.Event()
        self._closed = False
        self._out_buf = bytearray()
        self._reader_installed = False
        self._writer_installed = False
        self._loop = asyncio.get_running_loop()
        self._loop.add_reader(self._master_fd, self._on_readable)
        self._reader_installed = True
        self._wait_task = self._loop.create_task(self._watch_exit())

    # --- construction ---------------------------------------------------------------

    @classmethod
    async def spawn(
        cls, argv: Sequence[str], *, env: dict[str, str] | None = None, cwd: str | None = None,
        term: str = DEFAULT_TERM, rows: int = 24, cols: int = 80,
        ring_size: int = DEFAULT_RING_SIZE, queue_maxsize: int = DEFAULT_ATTACH_QUEUE_CHUNKS,
    ) -> PtySession:
        """Opens a fresh PTY pair, sizes it BEFORE spawning (so the child never sees a 0x0
        window even for an instant), spawns `argv` with the slave wired to all three of its
        stdio streams, and closes the parent's copy of the slave fd — the child kept its own
        across the fork, and a lingering parent-side copy would mean the slave never truly
        hangs up when the child exits (reads on the master would block/echo forever instead of
        raising EIO, which `_on_readable` depends on to notice a dead child)."""
        if queue_maxsize <= 0:  # validated HERE, before the pty and child exist — the
            raise ValueError(   # constructor's own check would fire too late and leak both
                f"queue_maxsize must be positive, got {queue_maxsize}")
        master_fd, slave_fd = os.openpty()
        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))

        full_env = dict(env) if env is not None else dict(os.environ)
        full_env["TERM"] = term

        try:
            proc = await asyncio.create_subprocess_exec(
                *argv, stdin=slave_fd, stdout=slave_fd, stderr=slave_fd, cwd=cwd, env=full_env,
                start_new_session=True, preexec_fn=_claim_controlling_tty,
            )
        finally:
            os.close(slave_fd)  # the PARENT's copy only — see the docstring above

        os.set_blocking(master_fd, False)
        return cls(
            master_fd=master_fd, proc=proc, rows=rows, cols=cols, ring_size=ring_size,
            queue_maxsize=queue_maxsize)

    # --- read side: master fd -> ring + live attachments ----------------------------

    def _on_readable(self) -> None:
        if not self._drain_available():
            self._stop_reading()

    def _drain_available(self, budget: int | None = _MAX_CHUNKS_PER_WAKEUP) -> bool:
        """Reads what's CURRENTLY buffered on the master, without blocking — a loop, not a
        single `os.read()`, because a busy child can queue more than one chunk between wakeups —
        but a BUDGETED loop (`_MAX_CHUNKS_PER_WAKEUP`; see the constant's comment for why an
        unbounded one is a doctrine-3 bug twice over). `budget=None` lifts the cap for
        `close()`'s final sweep, where the child is dead, the leftovers are finite, and EIO is
        the loop's natural floor. Returns True while the PTY is still open for reading (budget
        spent, or `BlockingIOError`: genuinely nothing left *right now* — either way the fd
        stays registered and the loop re-fires us); False once it hangs up (EIO — see the
        module docstring; on Linux this fires only once every already-buffered byte has been
        drained, never before, so no output is ever lost to the race between "child exits" and
        "we get around to reading")."""
        try:
            while budget is None or budget > 0:
                data = os.read(self._master_fd, _READ_CHUNK)
                if not data:  # EOF proper — not the common case (see EIO above) but honored
                    return False
                self._ring.append(data)
                self._fanout(data)
                if budget is not None:
                    budget -= 1
            return True  # budget spent, fd possibly still readable — the loop re-invokes us
        except BlockingIOError:
            return True
        except OSError:
            return False

    def _fanout(self, data: bytes) -> None:
        stalled: list[asyncio.Queue[bytes | None]] = []
        for queue in self._attachments:
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:  # this consumer stopped draining — it, alone, pays
                stalled.append(queue)  # collected, not detached mid-iteration
        for queue in stalled:
            self._force_detach(queue)

    def _force_detach(self, queue: asyncio.Queue[bytes | None]) -> None:
        """The slow-consumer casualty path (see the class docstring's BACKPRESSURE note): drop
        THIS attachment from the fan-out, discard its undeliverable backlog (the ring already
        holds those bytes — replay-on-reattach is the recovery, so keeping 16 MiB of chunks a
        dead peer will never read serves nobody), and terminate it with the `None` end-marker
        so its pump learns the stream is over the moment it next reads."""
        self._attachments.discard(queue)
        with contextlib.suppress(asyncio.QueueEmpty):
            while True:
                queue.get_nowait()
        queue.put_nowait(None)  # always fits: the queue was just cleared
        logger.warning(
            "pty attachment force-detached: consumer stalled at %d queued chunks "
            "(child pid=%d, %d attachment(s) remain); its recovery is reattach + ring replay",
            queue.maxsize, self._proc.pid, len(self._attachments))

    @staticmethod
    def _put_end_marker(queue: asyncio.Queue[bytes | None]) -> None:
        """Land the `None` end-marker even on a queue sitting exactly at its bound — dropping
        oldest chunks until it fits. The ring still holds every dropped byte (ring writes happen
        before fan-out); the marker is the one thing that must not be lost."""
        while True:
            try:
                queue.put_nowait(None)
                return
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()

    def _stop_reading(self) -> None:
        if self._reader_installed:
            with contextlib.suppress(ValueError):
                self._loop.remove_reader(self._master_fd)
            self._reader_installed = False

    # --- write side: attachments -> master fd ---------------------------------------

    def write(self, data: bytes) -> None:
        """Queues `data` for the master fd, installing a writer callback only while there is
        something outstanding — mirrors how `bodies.py`'s trampoline treats a corpse: a write
        after the child is gone just finds nobody listening (an `OSError` on the master, caught
        below) rather than crashing the broker."""
        if self._closed or not data:
            return
        self._out_buf.extend(data)
        if not self._writer_installed:
            self._loop.add_writer(self._master_fd, self._on_writable)
            self._writer_installed = True

    def _on_writable(self) -> None:
        try:
            n = os.write(self._master_fd, bytes(self._out_buf))
        except BlockingIOError:
            return  # the pty's write buffer is full; try again on the next wakeup
        except OSError:
            self._out_buf.clear()  # nobody left to read it — an honest drop, not a crash
            self._stop_writing()
            return
        del self._out_buf[:n]
        if not self._out_buf:
            self._stop_writing()

    def _stop_writing(self) -> None:
        if self._writer_installed:
            with contextlib.suppress(ValueError):
                self._loop.remove_writer(self._master_fd)
            self._writer_installed = False

    # --- resize -----------------------------------------------------------------------

    def resize(self, rows: int, cols: int) -> None:
        """`TIOCSWINSZ` on the master — the kernel mirrors the new size onto the slave and, if
        it actually changed, delivers `SIGWINCH` to the slave's foreground process group all by
        itself (line-discipline behavior, not something this module has to simulate); see the
        module docstring for why that delivery depends on `spawn()`'s `TIOCSCTTY` dance having
        actually claimed a controlling terminal."""
        if self._closed:
            return
        fcntl.ioctl(self._master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        self._rows, self._cols = rows, cols

    # --- attach / detach ----------------------------------------------------------------

    def attach(self) -> tuple[bytes, asyncio.Queue[bytes | None]]:
        """Returns `(replay, queue)`: `replay` is the ring's current contents — the instant
        repaint a reattaching face wants instead of staring at black — and `queue` receives
        every live chunk from THIS moment on, plus a single `None` sentinel the moment the
        child exits (immediately, if it already has: attaching to a corpse still tells you
        it's a corpse). Multiple simultaneous attachments are just multiple queues in the same
        set — nothing about one affects another, INCLUDING one of them stalling: the queue is
        bounded (`queue_maxsize` chunks), and a consumer that lets it fill is force-detached
        alone (class docstring, BACKPRESSURE) while its siblings stream on."""
        replay = self._ring.snapshot()
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=self._queue_maxsize)
        self._attachments.add(queue)
        if self._exited.is_set():
            queue.put_nowait(None)
        return replay, queue

    def detach(self, queue: asyncio.Queue[bytes | None]) -> None:
        """Unsubscribes `queue`. Never touches the child — detach/reattach are tmux semantics
        (§2): the session is the durable thing, an attachment is just a spectator."""
        self._attachments.discard(queue)

    # --- lifecycle --------------------------------------------------------------------

    async def _watch_exit(self) -> None:
        """Runs for the session's whole life, in the background: the SINGLE place that learns
        the child died (however it died — naturally, or via `close()`'s signals) and fans the
        exited marker out to every attachment, including ones that arrive after the fact (see
        `attach()`)."""
        await self._proc.wait()
        self._exited.set()
        for queue in self._attachments:
            self._put_end_marker(queue)  # lands even on a queue sitting exactly at its bound

    @property
    def returncode(self) -> int | None:
        return self._proc.returncode

    @property
    def pid(self) -> int:
        return self._proc.pid

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def cols(self) -> int:
        return self._cols

    @property
    def exited(self) -> asyncio.Event:
        return self._exited

    @property
    def attach_count(self) -> int:
        return len(self._attachments)

    @property
    def ring_bytes(self) -> int:
        return len(self._ring)

    async def close(self, *, grace: float = 2.0) -> None:
        """Terminates the child (SIGHUP, then SIGKILL after `grace` seconds if it ignored the
        first), drains whatever it still buffered, and releases the PTY fd. Idempotent — a
        second call is a no-op, never a double-signal or a double-close. Signals go to the
        whole process GROUP (`os.killpg`, valid here because `start_new_session` made the child
        both its own session AND process-group leader — POSIX `setsid()` sets `pgid == pid`),
        not just the one PID: a shell that spawned children of its own inside this PTY should
        not survive its session leader's death."""
        if self._closed:
            return
        self._closed = True
        if self._proc.returncode is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(self._proc.pid, signal.SIGHUP)
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=grace)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(self._proc.pid, signal.SIGKILL)
                await self._proc.wait()
        await self._wait_task  # the exit fan-out (see _watch_exit) has definitely run by now
        # a final, UNBUDGETED sweep (the child is dead; leftovers are finite; EIO is the floor)
        # — harmless if _on_readable already caught the EIO
        self._drain_available(budget=None)
        self._stop_reading()
        self._stop_writing()
        with contextlib.suppress(OSError):
            os.close(self._master_fd)


# --- the wire protocol (client is `src/manager/attach.py`; this is the server half) --------
#
# One JSON "hello" line, newline-terminated, sent once by the client right after connecting:
#   {"name": <str>, "rows": <int>, "cols": <int>}
# Everything after the hello, BOTH directions, is a sequence of length-prefixed frames:
#   1 byte type tag + 4-byte big-endian length + that many bytes of payload.
#   'O' (server -> client): raw pty output — the replay first, then live, byte for byte.
#   'X' (server -> client): sent exactly once, JSON {"returncode": int|null} — the exited
#       marker (§4.5: a face attaching to a corpse still needs to be TOLD).
#   'I' (client -> server): raw bytes to write to the pty (this client's stdin).
#   'R' (client -> server): an out-of-band resize, JSON {"rows": int, "cols": int} — the ONE
#       message type that could otherwise be confused with a raw 'O'/'I' payload, which the
#       type tag makes structurally impossible.
# `attach.py` carries the same description at its own head, for whoever opens the client first.

FRAME_TYPE_OUTPUT = b"O"
FRAME_TYPE_EXITED = b"X"
FRAME_TYPE_INPUT = b"I"
FRAME_TYPE_RESIZE = b"R"

_FRAME_HEADER = struct.Struct(">cI")  # 1-byte type tag, 4-byte big-endian length — 5 bytes flat


def pack_frame(frame_type: bytes, payload: bytes) -> bytes:
    return _FRAME_HEADER.pack(frame_type, len(payload)) + payload


async def read_frame(reader: asyncio.StreamReader) -> tuple[bytes, bytes] | None:
    """One frame, or `None` on a clean disconnect (whether it lands between frames or mid-
    frame — `readexactly`'s `IncompleteReadError` covers both, and both mean the same thing to
    a caller: the other side is gone)."""
    try:
        header = await reader.readexactly(_FRAME_HEADER.size)
    except asyncio.IncompleteReadError:
        return None
    frame_type, length = _FRAME_HEADER.unpack(header)
    try:
        payload = await reader.readexactly(length)
    except asyncio.IncompleteReadError:
        return None
    return frame_type, payload


def encode_hello(name: str, rows: int, cols: int) -> bytes:
    return (json.dumps({"name": name, "rows": rows, "cols": cols}) + "\n").encode()


async def read_hello(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    """The one line before the framed stream starts. `None` on disconnect before it ever
    arrives; raises `ValueError`/`json.JSONDecodeError` (a `ValueError` subclass) on a
    malformed line — the caller decides whether that refuses the connection or crashes it."""
    line = await reader.readline()
    if not line:
        return None
    parsed: dict[str, Any] = json.loads(line.decode())
    return parsed


async def _pump_output(
    session: PtySession, queue: asyncio.Queue[bytes | None], writer: asyncio.StreamWriter,
) -> None:
    """One attachment's live feed -> its socket, framed. Runs until the `None` end-marker or
    until it's cancelled by the connection handler tearing down (client disconnected first).
    The marker means one of two things, told apart by the session itself: the child EXITED
    (returncode is set — send the 'X' frame, then close) or this attachment was FORCE-DETACHED
    for stalling (child still alive — close with no 'X': the EOF is the message, and the
    client's recovery is reconnect + ring replay). Either way the socket closes here, so the
    client always sees a clean EOF instead of a connection that silently stopped speaking.

    The per-frame `drain()` is THE backpressure link: it parks this pump when the client stops
    reading, which lets the session-side bounded queue fill and trip the force-detach — without
    it, the transport's own write buffer would just become the unbounded sink the queue bound
    exists to prevent."""
    while True:
        item = await queue.get()
        if item is None:
            if session.returncode is not None:
                payload = json.dumps({"returncode": session.returncode}).encode()
                writer.write(pack_frame(FRAME_TYPE_EXITED, payload))
                with contextlib.suppress(OSError):
                    await writer.drain()
            writer.close()
            return
        writer.write(pack_frame(FRAME_TYPE_OUTPUT, item))
        with contextlib.suppress(OSError):
            await writer.drain()


async def _serve_one_client(
    session: PtySession, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
) -> None:
    try:
        hello = await read_hello(reader)
    except ValueError:  # malformed hello — refuse this connection, never the broker/session
        hello = None
    if hello is None:
        writer.close()
        return
    with contextlib.suppress(KeyError, ValueError, TypeError):
        session.resize(int(hello["rows"]), int(hello["cols"]))

    replay, queue = session.attach()
    try:
        writer.write(pack_frame(FRAME_TYPE_OUTPUT, replay))  # the instant repaint
        await writer.drain()
        pump = asyncio.ensure_future(_pump_output(session, queue, writer))
        try:
            while True:
                frame = await read_frame(reader)
                if frame is None:
                    break
                frame_type, payload = frame
                if frame_type == FRAME_TYPE_INPUT:
                    session.write(payload)
                elif frame_type == FRAME_TYPE_RESIZE:
                    with contextlib.suppress(ValueError, KeyError, TypeError):
                        dims = json.loads(payload.decode())
                        session.resize(int(dims["rows"]), int(dims["cols"]))
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump
    finally:
        session.detach(queue)
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()


async def serve_session(session: PtySession, socket_path: str) -> asyncio.Server:
    """Serves ONE session on a unix socket at `socket_path` — enough to exercise the wire
    protocol end to end (`attach.py`'s client, or a bare `asyncio` stream pair in a test) before
    the wave-2 daemon exists to route many named sessions over many seats. Multiple clients may
    connect concurrently; `PtySession.attach()` already supports it, so this is just "one
    listener, `_serve_one_client` per connection, no shared state between them but the
    session"."""
    with contextlib.suppress(FileNotFoundError):
        os.unlink(socket_path)

    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _serve_one_client(session, reader, writer)

    return await asyncio.start_unix_server(_handler, path=socket_path)


# --- the registry -----------------------------------------------------------------------


class SessionExistsError(Exception):
    """`PtyBroker.spawn()` on a name collision — refuse loudly (this task's own rule), never
    silently replace a live session sitting under a name something else still expects."""


@dataclass(frozen=True)
class SessionInfo:
    """One row of `PtyBroker.list()` — everything a roster view needs without reaching into a
    `PtySession`'s internals."""

    name: str
    alive: bool
    rows: int
    cols: int
    ring_bytes: int
    attach_count: int


class PtyBroker:
    """A registry of named `PtySession`s. Deliberately thin: naming, lookup, and fan-out to
    `spawn`/`close` are ALL it does — the daemon (wave 2) owns policy (who may spawn what,
    warm/cold, ceilings); this module owns mechanism only."""

    def __init__(self, *, ring_size: int = DEFAULT_RING_SIZE) -> None:
        self._ring_size = ring_size
        self._sessions: dict[str, PtySession] = {}

    async def spawn(
        self, name: str, argv: Sequence[str], *, env: dict[str, str] | None = None,
        cwd: str | None = None, term: str = DEFAULT_TERM, rows: int = 24, cols: int = 80,
        ring_size: int | None = None, queue_maxsize: int = DEFAULT_ATTACH_QUEUE_CHUNKS,
    ) -> PtySession:
        if name in self._sessions:
            raise SessionExistsError(f"a PTY session named {name!r} already exists")
        session = await PtySession.spawn(
            argv, env=env, cwd=cwd, term=term, rows=rows, cols=cols,
            ring_size=ring_size if ring_size is not None else self._ring_size,
            queue_maxsize=queue_maxsize)
        self._sessions[name] = session
        return session

    def get(self, name: str) -> PtySession | None:
        return self._sessions.get(name)

    def list(self) -> list[SessionInfo]:
        return [
            SessionInfo(
                name=name, alive=session.returncode is None, rows=session.rows,
                cols=session.cols, ring_bytes=session.ring_bytes,
                attach_count=session.attach_count)
            for name, session in self._sessions.items()
        ]

    async def close(self, name: str) -> None:
        session = self._sessions.pop(name, None)
        if session is None:  # unknown name: idempotent no-op, same discipline as bodies.dissolve
            return
        await session.close()
