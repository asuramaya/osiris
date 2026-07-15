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
from pathlib import Path
from typing import Any

from src.manager.daemon import Manager, ScreamState, watchdog_tick

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
