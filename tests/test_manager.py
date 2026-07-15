"""osiris-manager — the sacred proc's control socket, re-adoption, and the scream.

Never real systemd or DBus in a unit test: `_make_runner` fakes the ONE `ProcessRunner` seam
this daemon and `bodies.LocalProvider` share (mirrors `tests/test_bodies.py`'s `_fake_runner`),
and the watchdog's `probe`/`notify` are pure injected callables — `watchdog_tick` is tested with
neither Postgres, Redis, nor a running Manager anywhere nearby. The one test that DOES touch a
real graph (`test_default_probe_reads_a_real_postgres_and_redis`) uses the testcontainers
fixtures every other suite in this repo already relies on, never mocks.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
from typing import Any

import pytest
from src.manager.daemon import Manager, ScreamState, watchdog_tick
from src.manager.pty_broker import FRAME_TYPE_INPUT, FRAME_TYPE_OUTPUT, pack_frame, read_frame

requires_pty = pytest.mark.skipif(
    not hasattr(os, "openpty"), reason="os.openpty() is unavailable on this platform")

# --- the fake ProcessRunner: the one seam both the daemon and LocalProvider share ---


class _FakeProc:
    """A stand-in for the tail of `asyncio.subprocess.Process` the daemon actually touches
    (`.communicate()`, `.wait()`, `.returncode`) — same idiom as test_bodies.py's
    `_FakeCompletedProc`."""

    def __init__(self, out: bytes = b"", returncode: int = 0) -> None:
        self._out = out
        self.returncode = returncode

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._out, b""

    async def wait(self) -> int:
        return self.returncode


def _make_runner(
    *, units_json: str = "[]", cgroup_dir: Path | None = None, is_active: str = "inactive",
    busctl_rc: int = 0,
) -> Any:
    """Answers every subprocess call this daemon can make: `list-units` (canned JSON),
    `show -p ControlGroup` (a fixture cgroup dir), `stop` (always succeeds), `is-active`
    (canned state), `notify-send` (always "succeeds" — the scream tests exercise the notifier
    directly, never through this runner)."""
    calls: list[list[str]] = []

    async def runner(
        argv: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None,
        stdout: int | None = None, stderr: int | None = None,
    ) -> _FakeProc:
        calls.append(argv)
        if argv[:3] == ["systemctl", "--user", "list-units"]:
            return _FakeProc(units_json.encode())
        if argv[:3] == ["systemctl", "--user", "show"] and "ControlGroup" in argv:
            return _FakeProc(str(cgroup_dir).encode() if cgroup_dir else b"")
        if argv[:3] == ["systemctl", "--user", "stop"]:
            return _FakeProc(b"")
        if len(argv) >= 3 and argv[2] == "is-active":
            return _FakeProc(is_active.encode())
        if argv and argv[0] == "notify-send":
            return _FakeProc(b"")
        if argv and argv[0] == "busctl":
            return _FakeProc(b"", returncode=busctl_rc)
        return _FakeProc(b"")

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


# --- a tiny client for the newline-delimited JSON protocol ---


class _Client:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def send(self, req: dict[str, Any]) -> dict[str, Any]:
        self._writer.write((json.dumps(req) + "\n").encode())
        await self._writer.drain()
        line = await self._reader.readline()
        result: dict[str, Any] = json.loads(line)
        return result

    async def close(self) -> None:
        self._writer.close()
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()


async def _connect(path: Path) -> _Client:
    reader, writer = await asyncio.open_unix_connection(path=str(path))
    return _Client(reader, writer)


# --- a tiny client for the pty_attach handshake + the framed protocol it hands off into ---


async def _attach(
    path: Path, name: str, *, rows: int = 24, cols: int = 80,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, dict[str, Any]]:
    """Opens a FRESH connection (pty_attach is a one-shot switch, never shared with the JSON
    control connection that requested it — see `daemon._op_pty_attach`), sends the request, and
    returns the still-open (reader, writer) plus the one JSON ack line the daemon answers with
    before handing the connection to the framed stream."""
    reader, writer = await asyncio.open_unix_connection(path=str(path))
    req = {"op": "pty_attach", "name": name, "rows": rows, "cols": cols}
    writer.write((json.dumps(req) + "\n").encode())
    await writer.drain()
    ack: dict[str, Any] = json.loads(await reader.readline())
    return reader, writer, ack


async def _read_frames_until(reader: asyncio.StreamReader, needle: bytes) -> bytes:
    """Reads framed 'O' output until `needle` shows up — bounded by the CALLER's own
    `asyncio.timeout`, same discipline as `test_pty_broker.py`'s `_read_output_until`: an
    event-driven wait on the next frame, never a sleep, never a fixed delay."""
    buf = bytearray()
    while needle not in buf:
        frame = await read_frame(reader)
        assert frame is not None
        frame_type, payload = frame
        assert frame_type == FRAME_TYPE_OUTPUT
        buf.extend(payload)
    return bytes(buf)


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


# --- status / bodies / dissolve round-trip ---


async def test_status_bodies_dissolve_round_trip(tmp_path: Path) -> None:
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "cpu.stat").write_text("usage_usec 500000\n")
    (cgroup / "memory.peak").write_text("4096\n")
    (cgroup / "memory.events").write_text("oom_kill 0\n")
    units = json.dumps([{"unit": "osiris-body-h1.scope", "load": "loaded",
                          "active": "active", "sub": "running"}])
    receipts = tmp_path / "receipts"
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=receipts,
        # cgroup_root=Path("/"): the fake runner hands back the fixture dir's own ABSOLUTE
        # path as the "ControlGroup" value, same trick test_bodies.py's `_fake_runner` uses —
        # joined onto "/" it resolves to exactly that path instead of nesting under a real
        # /sys/fs/cgroup that doesn't exist in the fixture.
        cgroup_root=Path("/"),
        runner=_make_runner(units_json=units, cgroup_dir=cgroup, is_active="inactive"))
    await manager.start()
    try:
        client = await _connect(manager._socket_path)
        try:
            status = await client.send({"op": "status"})
            assert status["adopted_bodies"] == 1
            # no pool/redis wired at all: the default probe reads healthy, never a
            # fabricated down signal nothing actually observed.
            assert status["graph_healthy"] is True
            assert status["last_scream"] is None
            assert isinstance(status["uptime_seconds"], float)

            bodies_resp = await client.send({"op": "bodies"})
            assert bodies_resp == {
                "bodies": [{"handle": "h1", "unit": "osiris-body-h1.scope", "state": "active"}]}

            dissolved = await client.send({"op": "dissolve", "handle": "h1"})
            assert dissolved == {"dissolved": "h1"}

            status2 = await client.send({"op": "status"})
            assert status2["adopted_bodies"] == 0

            gone = await client.send({"op": "bodies"})
            assert gone == {"bodies": []}

            missing = await client.send({"op": "dissolve", "handle": "h1"})
            assert "error" in missing  # already dissolved: unknown handle now, not a crash

            unknown_op = await client.send({"op": "reticulate"})
            assert "error" in unknown_op
        finally:
            await client.close()
    finally:
        await manager.close()

    # THE ONE MUTATING OP IN v1 goes through LocalProvider — receipted, exactly Phase 0's lane.
    receipt = json.loads((receipts / "h1.json").read_text())
    assert receipt["handle"] == "h1"
    assert receipt["provider"] == "local"
    assert receipt["core_seconds"] == 0.5           # off the fixture cgroup (fallback lane)
    assert receipt["ram_peak_bytes"] == 4096


async def test_unknown_op_and_malformed_json_error_without_crashing(tmp_path: Path) -> None:
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    await manager.start()
    try:
        reader, writer = await asyncio.open_unix_connection(path=str(manager._socket_path))
        writer.write(b"not json at all\n")
        await writer.drain()
        resp1 = json.loads(await reader.readline())
        assert "error" in resp1

        writer.write((json.dumps({"no_op_field": True}) + "\n").encode())
        await writer.drain()
        resp2 = json.loads(await reader.readline())
        assert "error" in resp2

        writer.write((json.dumps({"op": "dissolve"}) + "\n").encode())  # no handle at all
        await writer.drain()
        resp3 = json.loads(await reader.readline())
        assert "error" in resp3

        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
    finally:
        await manager.close()


async def test_socket_is_mode_0600_and_removed_on_close(tmp_path: Path) -> None:
    path = tmp_path / "m.sock"
    manager = Manager(socket_path=path, receipts_dir=tmp_path / "receipts", runner=_make_runner())
    await manager.start()
    assert path.exists()
    assert (path.stat().st_mode & 0o777) == 0o600
    await manager.close()
    assert not path.exists()


# --- re-adoption: STATE IS RECONSTRUCTED, NEVER TRUSTED FROM MEMORY ---


async def test_a_live_unit_from_a_dead_daemon_is_readopted(tmp_path: Path) -> None:
    units = json.dumps([
        {"unit": "osiris-body-abc123.scope", "load": "loaded",
         "active": "active", "sub": "running"},
    ])
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner(units_json=units))
    await manager.start()
    try:
        client = await _connect(manager._socket_path)
        try:
            resp = await client.send({"op": "bodies"})
        finally:
            await client.close()
    finally:
        await manager.close()
    assert resp == {
        "bodies": [{"handle": "abc123", "unit": "osiris-body-abc123.scope", "state": "active"}]}


async def test_a_unit_with_an_existing_receipt_is_reaped_not_readopted(tmp_path: Path) -> None:
    receipts = tmp_path / "receipts"
    receipts.mkdir(parents=True)
    (receipts / "dead1.json").write_text('{"v": 1, "handle": "dead1"}')
    units = json.dumps([
        {"unit": "osiris-body-dead1.scope", "active": "inactive"},   # already dissolved
        {"unit": "osiris-body-live2.scope", "active": "active"},     # outlived the old daemon
    ])
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=receipts,
        runner=_make_runner(units_json=units))
    await manager.start()
    try:
        client = await _connect(manager._socket_path)
        try:
            resp = await client.send({"op": "bodies"})
        finally:
            await client.close()
    finally:
        await manager.close()
    handles = {b["handle"] for b in resp["bodies"]}
    assert handles == {"live2"}  # dead1 has a receipt already: reaped, never re-adopted


async def test_unparseable_list_units_output_degrades_to_an_empty_registry(
    tmp_path: Path,
) -> None:
    """A systemctl hiccup at boot must never crash the daemon — an empty registry (nothing
    adopted this run) is the honest, safe degradation."""
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner(units_json="not json"))
    await manager.start()
    try:
        client = await _connect(manager._socket_path)
        try:
            resp = await client.send({"op": "bodies"})
        finally:
            await client.close()
    finally:
        await manager.close()
    assert resp == {"bodies": []}


# --- the scream: edge-triggered, never level-triggered, never silent ---


async def test_watchdog_screams_exactly_once_across_a_multi_tick_outage_then_all_clears() -> None:
    calls: list[tuple[str, str, str]] = []

    async def notify(urgency: str, summary: str, body: str) -> None:
        calls.append((urgency, summary, body))

    results = iter([True, True, False, False, False, True, True])  # up, up, DOWN x3, UP

    async def probe() -> bool:
        return next(results)

    state = ScreamState()
    for _ in range(7):
        await watchdog_tick(state, probe, notify)

    urgencies = [c[0] for c in calls]
    assert urgencies == ["critical", "normal"]  # ONE scream for the whole outage, ONE all-clear
    assert state.healthy is True
    assert state.last_event is not None
    assert state.last_event["kind"] == "recovered"
    assert state.last_event["outage_seconds"] is not None


async def test_watchdog_never_screams_while_healthy_throughout() -> None:
    calls: list[str] = []

    async def notify(urgency: str, summary: str, body: str) -> None:
        calls.append(urgency)

    async def probe() -> bool:
        return True

    state = ScreamState()
    for _ in range(5):
        await watchdog_tick(state, probe, notify)
    assert calls == []


async def test_watchdog_screams_on_the_very_first_tick_if_already_down() -> None:
    """NEVER SILENTLY DEGRADE (doctrine 3): a graph that is already down when the daemon
    boots must scream on the first observation, not wait for a transition that already
    happened before anyone was watching."""
    calls: list[str] = []

    async def notify(urgency: str, summary: str, body: str) -> None:
        calls.append(urgency)

    async def probe() -> bool:
        return False

    state = ScreamState()
    await watchdog_tick(state, probe, notify)
    assert calls == ["critical"]


async def test_status_reports_the_last_scream(tmp_path: Path) -> None:
    """`{"op": "status"}` surfaces the watchdog's memory (`self._scream`), not a fresh probe.
    Exercised directly against `_op_status()` — a live watchdog task would race a manual
    `_scream` mutation the instant it next ticks, which is exactly the kind of flake this
    daemon's real behavior (state driven ONLY by `watchdog_tick`) should never have; the
    round-trip test above already proves the socket wiring end to end."""
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    manager._scream.healthy = False
    manager._scream.last_event = {"kind": "down", "at": "2026-07-15T00:00:00+00:00",
                                   "outage_seconds": None}
    status = manager._op_status()
    assert status["graph_healthy"] is False
    assert status["last_scream"] == {
        "kind": "down", "at": "2026-07-15T00:00:00+00:00", "outage_seconds": None}


# --- the default probe against a REAL Postgres + Redis (testcontainers, not mocks) ---


async def test_default_probe_reads_a_real_postgres_and_redis(
    pg_dsn: str, redis_url: str, tmp_path: Path,
) -> None:
    from src.db.pool import create_pool
    from src.db.redis import create_redis

    pool = await create_pool(pg_dsn)
    redis_client = create_redis(redis_url)
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        pool=pool, redis=redis_client, runner=_make_runner())
    await manager.start()
    try:
        assert await manager._default_probe() is True
    finally:
        await manager.close()
        await pool.close()
        await redis_client.aclose()


async def test_default_probe_reports_down_when_postgres_is_unreachable(
    redis_url: str, tmp_path: Path,
) -> None:
    import asyncpg
    from src.db.redis import create_redis

    # A pool pointed at a port nothing listens on: min_size=0 so construction itself
    # never blocks — the failure surfaces inside the probe's own bounded timeout, not here.
    pool = await asyncpg.create_pool(
        dsn="postgresql://osiris:osiris@127.0.0.1:1/osiris", min_size=0, max_size=1)
    assert pool is not None
    redis_client = create_redis(redis_url)
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        pool=pool, redis=redis_client, runner=_make_runner())
    await manager.start()
    try:
        assert await manager._default_probe() is False
    finally:
        await manager.close()
        await pool.close()
        await redis_client.aclose()


# --- the PTY broker wired into the daemon's control socket (§4.5, tmux-with-a-graph) ---
#
# No fakes here — a real Manager, a real PtyBroker, real ptys/children (mirrors
# test_pty_broker.py's own discipline: the whole point is proving a raw fd behaves like a
# terminal end to end over THIS daemon's socket, which a fake subprocess cannot prove).
# Synchronization is always on OBSERVABLE STATE — a marker the child itself echoes, or
# `attach_count` read directly off the broker's own session object — never a sleep, never a
# race against how fast a shell happens to start.


@requires_pty
async def test_pty_spawn_then_list_shows_it_and_rejects_a_name_collision(
    tmp_path: Path,
) -> None:
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    await manager.start()
    try:
        client = await _connect(manager._socket_path)
        try:
            spawned = await client.send(
                {"op": "pty_spawn", "name": "s1", "argv": ["sh", "-c", "cat"]})
            assert spawned == {"spawned": "s1"}

            # a name collision errors LOUDLY — never a silent replace of the live session.
            collided = await client.send(
                {"op": "pty_spawn", "name": "s1", "argv": ["sh", "-c", "cat"]})
            assert "error" in collided
            assert manager._broker.get("s1") is not None  # the original session, untouched

            listed = await client.send({"op": "pty_list"})
            assert listed == {
                "sessions": [
                    {"name": "s1", "alive": True, "rows": 24, "cols": 80, "attach_count": 0}]}

            closed = await client.send({"op": "pty_close", "name": "s1"})
            assert closed == {"closed": "s1"}
            assert await client.send({"op": "pty_list"}) == {"sessions": []}
        finally:
            await client.close()
    finally:
        await manager.close()


@requires_pty
async def test_pty_spawn_rejects_a_malformed_request_without_touching_the_broker(
    tmp_path: Path,
) -> None:
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    await manager.start()
    try:
        client = await _connect(manager._socket_path)
        try:
            no_name = await client.send({"op": "pty_spawn", "argv": ["sh"]})
            assert "error" in no_name
            no_argv = await client.send({"op": "pty_spawn", "name": "x"})
            assert "error" in no_argv
            empty_argv = await client.send({"op": "pty_spawn", "name": "x", "argv": []})
            assert "error" in empty_argv
            assert await client.send({"op": "pty_list"}) == {"sessions": []}
        finally:
            await client.close()
    finally:
        await manager.close()


@requires_pty
async def test_pty_attach_streams_output_over_the_daemon_socket_and_input_round_trips(
    tmp_path: Path,
) -> None:
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    await manager.start()
    try:
        control = await _connect(manager._socket_path)
        try:
            spawned = await control.send(
                {"op": "pty_spawn", "name": "a1",
                 "argv": ["sh", "-c", "echo readymark; cat"]})
            assert spawned == {"spawned": "a1"}

            reader, writer, ack = await _attach(manager._socket_path, "a1")
            try:
                assert ack == {"attached": "a1"}
                async with asyncio.timeout(5.0):
                    await _read_frames_until(reader, b"readymark")

                writer.write(pack_frame(FRAME_TYPE_INPUT, b"echo inputmark\n"))
                await writer.drain()
                async with asyncio.timeout(5.0):
                    await _read_frames_until(reader, b"inputmark")
            finally:
                await _close_writer(writer)

            closed = await control.send({"op": "pty_close", "name": "a1"})
            assert closed == {"closed": "a1"}
        finally:
            await control.close()
    finally:
        await manager.close()


@requires_pty
async def test_two_attachments_to_one_session_both_receive_output(tmp_path: Path) -> None:
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    await manager.start()
    try:
        control = await _connect(manager._socket_path)
        try:
            spawned = await control.send(
                {"op": "pty_spawn", "name": "a2",
                 "argv": ["sh", "-c", "echo readymark; cat"]})
            assert spawned == {"spawned": "a2"}

            reader1, writer1, ack1 = await _attach(manager._socket_path, "a2")
            reader2, writer2, ack2 = await _attach(manager._socket_path, "a2")
            try:
                assert ack1 == {"attached": "a2"}
                assert ack2 == {"attached": "a2"}
                # both replays are guaranteed to carry "readymark": the ring holds it from the
                # moment it was first observed, and ring writes happen before any fan-out —
                # no race against how fast the shell started (see the section banner above).
                async with asyncio.timeout(5.0):
                    await _read_frames_until(reader1, b"readymark")
                async with asyncio.timeout(5.0):
                    await _read_frames_until(reader2, b"readymark")

                # OBSERVABLE STATE, not a guess: the broker's own session object says two
                # attachments are live before a single byte of the fan-out test is trusted.
                session = manager._broker.get("a2")
                assert session is not None
                assert session.attach_count == 2

                writer1.write(pack_frame(FRAME_TYPE_INPUT, b"echo fanoutmark\n"))
                await writer1.drain()
                async with asyncio.timeout(5.0):
                    await _read_frames_until(reader1, b"fanoutmark")
                async with asyncio.timeout(5.0):
                    await _read_frames_until(reader2, b"fanoutmark")
            finally:
                await _close_writer(writer1)
                await _close_writer(writer2)

            closed = await control.send({"op": "pty_close", "name": "a2"})
            assert closed == {"closed": "a2"}
        finally:
            await control.close()
    finally:
        await manager.close()


@requires_pty
async def test_pty_close_reaps_the_child_and_a_second_close_errors_loudly(
    tmp_path: Path,
) -> None:
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    await manager.start()
    try:
        client = await _connect(manager._socket_path)
        try:
            spawned = await client.send(
                {"op": "pty_spawn", "name": "a3", "argv": ["sh", "-c", "cat"]})
            assert spawned == {"spawned": "a3"}
            session = manager._broker.get("a3")
            assert session is not None
            assert session.returncode is None  # alive, before the close

            closed = await client.send({"op": "pty_close", "name": "a3"})
            assert closed == {"closed": "a3"}
            assert manager._broker.get("a3") is None  # reaped out of the registry
            assert session.returncode is not None  # the child was actually terminated

            # unknown name (already closed): errors loudly, never a silent no-op — same
            # discipline BUILD item 1 asks of every named-session op on this socket.
            second_close = await client.send({"op": "pty_close", "name": "a3"})
            assert "error" in second_close
        finally:
            await client.close()
    finally:
        await manager.close()


@requires_pty
async def test_pty_attach_to_an_unknown_name_errors_and_leaves_the_connection_usable(
    tmp_path: Path,
) -> None:
    manager = Manager(
        socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
        runner=_make_runner())
    await manager.start()
    try:
        reader, writer, ack = await _attach(manager._socket_path, "does-not-exist")
        try:
            assert "error" in ack

            # the connection was NEVER switched into frame mode: it still speaks ordinary
            # newline-JSON control, proving the error path never silently handed it off.
            writer.write((json.dumps({"op": "status"}) + "\n").encode())
            await writer.drain()
            status = json.loads(await reader.readline())
            assert "uptime_seconds" in status
        finally:
            await _close_writer(writer)
    finally:
        await manager.close()


# --- identity at birth (§4.2, ruling 5cef856b): pty_spawn naming a seat ---


async def test_pty_spawn_with_seat_refuses_without_graph_access(tmp_path: Path) -> None:
    """A body summoned for a seat that cannot be minted must not be born unbound — a daemon
    with no pool refuses BEFORE any fd or child exists, and a malformed seat is refused the
    same way."""
    manager = Manager(socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
                      runner=_make_runner())
    await manager.start()
    try:
        client = await _connect(tmp_path / "m.sock")
        try:
            out = await client.send({"op": "pty_spawn", "name": "s1",
                                     "argv": ["sh", "-c", "cat"],
                                     "seat": {"handle": "Horus", "house": "osiris"}})
            assert "graph access" in out["error"]
            assert manager._broker.get("s1") is None  # refused before a child ever existed
            malformed = await client.send({"op": "pty_spawn", "name": "s2",
                                           "argv": ["sh", "-c", "cat"],
                                           "seat": {"house": "osiris"}})
            assert "'seat'" in malformed["error"]
            assert manager._broker.get("s2") is None
        finally:
            await client.close()
    finally:
        await manager.close()


@requires_pty
async def test_pty_spawn_with_seat_exports_identity_at_birth(
    pg_dsn: str, tmp_path: Path,
) -> None:
    """The ceremony's daemon half (§4.2): a spawn naming a seat gets the Seat minted in the
    graph and OSIRIS_SEAT_ID + a ONE-TIME token exported into the child's environment before
    its first breath — witnessed here by the child echoing both back through its own pty,
    and by the token row sitting minted-but-unused (the whisper, not the daemon, spends it)."""
    from src.db.pool import create_pool

    pool = await create_pool(pg_dsn)
    manager = Manager(socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
                      pool=pool, runner=_make_runner())
    await manager.start()
    try:
        client = await _connect(tmp_path / "m.sock")
        try:
            out = await client.send({
                "op": "pty_spawn", "name": "seated",
                "argv": ["sh", "-c", 'echo "B=$OSIRIS_SEAT_ID:$OSIRIS_ATTACH_TOKEN"; cat'],
                "seat": {"handle": "Horus", "house": "osiris"}})
            assert out.get("spawned") == "seated"
            seat_id = out["seat_id"]
            assert seat_id.startswith("seat:")
            # the graph half: the Seat object exists; the token is minted and UNUSED
            assert await pool.fetchval(
                "SELECT 1 FROM objects WHERE canonical=$1 AND type='Seat'", seat_id)
            row = await pool.fetchrow(
                "SELECT token, used_at, minted_by FROM seat_tokens WHERE seat_id=$1", seat_id)
            assert row is not None and row["used_at"] is None
            assert row["minted_by"] == "osiris-manager"
            # the child half: both vars crossed the exec boundary, witnessed on the pty
            reader, writer, ack = await _attach(tmp_path / "m.sock", "seated")
            assert ack.get("attached") == "seated"
            async with asyncio.timeout(10):
                seen = await _read_frames_until(reader, b"B=seat:")
            assert row["token"].encode() in seen
            await _close_writer(writer)
        finally:
            await client.close()
    finally:
        await manager.close()
        await pool.close()


# --- the PTY envelope (the doctrine-3 gap, closed): a child is born bounded or not at all ---


@requires_pty
async def test_pty_spawn_moves_the_child_into_its_own_scope(tmp_path: Path) -> None:
    """Every PTY child is ADOPTED into its own transient scope right after birth
    (StartTransientUnit with PIDs= via busctl) — the spawn/wait/kill topology untouched,
    only the cgroup membership changes. The busctl call carries the child's REAL pid and
    the envelope (MemoryMax + IOWeight)."""
    runner = _make_runner()
    manager = Manager(socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
                      runner=runner)
    await manager.start()
    try:
        client = await _connect(tmp_path / "m.sock")
        try:
            out = await client.send({"op": "pty_spawn", "name": "bounded",
                                     "argv": ["sh", "-c", "cat"]})
            assert out.get("spawned") == "bounded"
            session = manager._broker.get("bounded")
            assert session is not None and session.pid > 0
            busctl = [c for c in runner.calls if c and c[0] == "busctl"]
            assert len(busctl) == 1
            call = busctl[0]
            assert "osiris-pty-bounded.scope" in call
            assert str(session.pid) in call
            assert "MemoryMax" in call and "IOWeight" in call and "PIDs" in call
        finally:
            await client.close()
    finally:
        await manager.close()


@requires_pty
async def test_pty_spawn_refuses_a_child_it_cannot_bound(tmp_path: Path) -> None:
    """The strict half: a child whose scope adoption fails is CLOSED and the spawn refused —
    under the sacred proc a child is born bounded or not at all, never left as a limb of
    the daemon's own slice."""
    manager = Manager(socket_path=tmp_path / "m.sock", receipts_dir=tmp_path / "receipts",
                      runner=_make_runner(busctl_rc=1))
    await manager.start()
    try:
        client = await _connect(tmp_path / "m.sock")
        try:
            out = await client.send({"op": "pty_spawn", "name": "unbounded",
                                     "argv": ["sh", "-c", "cat"]})
            assert "could not be bounded" in out["error"]
            assert manager._broker.get("unbounded") is None   # closed, not lingering
        finally:
            await client.close()
    finally:
        await manager.close()
