"""osiris-manager — THE SACRED PROC (constitution #2, ruling `f1803b4a`; availability doctrine
`2ceb7ba0`). See `docs/SPEC-agent-manager.md` §1 (doctrines), §4.0, §4.4, §4.8.

v1 is a SKELETON: it owns ADOPTION + VISIBILITY + THE SCREAM. It does not yet summon seats
(that is wave 2) — every body it can ever act on in this version was summoned by something
else (`_spawn_in_body`, today; a later wave's warm-boot resurrection, eventually) and is only
ever seen here as something to RE-ADOPT.

STATE IS RECONSTRUCTED, NEVER TRUSTED FROM MEMORY. Daemon-death is a normal event — the whole
point of the sacred-proc doctrine is that a crash here costs nothing but this process: on
start, `Manager._rebuild_world` walks `systemctl --user list-units 'osiris-body-*'` and
cross-references `~/.osiris/body-receipts` to tell a body still running (RE-ADOPT it) from one
already dissolved (a receipt exists — nothing to do). COLD BY DEFAULT (doctrine 1): nothing is
re-bodied, resumed, or resurrected here — adoption only means "this daemon now KNOWS the scope
exists", never "make something alive again".

THE ONE MUTATING OP IN v1 is `dissolve`, and it goes through `bodies.LocalProvider` — the exact
Phase 0 lane (metered, receipted, receipt-before-return). Re-adopted bodies never had their
child handle in THIS process (a dead daemon held it), so `LocalProvider.adopt()` seeds `_live`
with a `_AdoptedProc` stand-in (systemctl-polling `.wait()`) instead of a real subprocess
handle — everything downstream (stats-file lookup, cgroup fallback, receipt mint) is the
SAME code Phase 0 already shipped and tested.

SACRED-PROC RULES IN CODE: this module spawns no long-lived children. Every subprocess it runs
(`systemctl`, `notify-send`) goes through the single injected `ProcessRunner` seam and is
awaited under a bound (`_systemctl_timeout` / `_stop_timeout` / a flat 5s for notify-send) —
never fire-and-forget, never unbounded.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shutil
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import redis.asyncio as aioredis

from src.config.settings import get_settings
from src.db.pool import create_pool
from src.db.redis import create_redis
from src.manager.pty_broker import PtyBroker, SessionExistsError, serve_attached
from src.orchestrator.bodies import RECEIPTS as BODY_RECEIPTS
from src.orchestrator.bodies import LocalProvider, ProcessRunner

_log = logging.getLogger("osiris.manager")

# cgroup v2's unified mount — mirrors `bodies._CGROUP_ROOT` (not imported: that name is
# private to bodies.py, and this is the one constant worth a one-line duplicate rather than
# reaching across the module boundary for it).
_CGROUP_ROOT = Path("/sys/fs/cgroup")

# How long `dissolve()` (via `_AdoptedProc.wait()`) will poll `systemctl --user is-active`
# before giving up and recording an honest "we don't actually know" — bounded, never hung
# (sacred-proc rule: every subprocess this daemon runs, including a poll loop, has a ceiling).
_DEFAULT_STOP_TIMEOUT = 30.0
_ADOPTED_POLL_INTERVAL = 0.2
_DEFAULT_SYSTEMCTL_TIMEOUT = 5.0
_DEFAULT_WATCHDOG_INTERVAL = 10.0
_NOTIFY_TIMEOUT = 5.0
_PROBE_TIMEOUT = 2.0


def default_socket_path() -> Path:
    """$XDG_RUNTIME_DIR/osiris-manager.sock, falling back to ~/.osiris/manager.sock — a unix
    domain socket only (ruling `d6403d34`: NOTHING listens on TCP)."""
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
    if runtime_dir:
        return Path(runtime_dir) / "osiris-manager.sock"
    return Path.home() / ".osiris" / "manager.sock"


async def _default_runner(
    argv: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None,
    stdout: int | None = None, stderr: int | None = None,
) -> asyncio.subprocess.Process:
    """The real seam: identical shape to `bodies._default_runner` (same `ProcessRunner`
    protocol) so `LocalProvider` and this daemon's own `systemctl`/`notify-send` calls share
    ONE injectable boundary — tests fake this once, never real systemd or DBus."""
    return await asyncio.create_subprocess_exec(
        *argv, cwd=cwd, env=env, stdout=stdout, stderr=stderr)


class _AdoptedProc:
    """A `bodies._Waitable` stand-in for a RE-ADOPTED scope. `LocalProvider.dissolve()` expects
    `proc.returncode` / `await proc.wait()` — the real (summon-time) case is a genuine
    `asyncio.subprocess.Process`, but a re-adopted body's original child handle belonged to a
    now-dead daemon process, which THIS process never parented (you cannot `wait()` a PID that
    isn't your child). So `.wait()` polls `systemctl --user is-active` instead, bounded by
    `timeout` — the same "never unbounded" rule as every other subprocess this daemon runs."""

    def __init__(self, unit: str, runner: ProcessRunner, *, timeout: float) -> None:
        self._unit = unit
        self._runner = runner
        self._timeout = timeout
        self.returncode: int | None = None

    async def wait(self) -> int:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        while True:
            proc = await self._runner(
                ["systemctl", "--user", "is-active", f"{self._unit}.scope"],
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await proc.communicate()
            state = out.decode().strip()
            if state != "active":
                # "failed" maps to a nonzero code (an honest signal something went wrong);
                # anything else absent/"inactive"/"unknown" reads as a clean stop.
                self.returncode = 1 if state == "failed" else 0
                return self.returncode
            if loop.time() >= deadline:
                self.returncode = -1  # wedged past the bound: honest, never a hang
                return self.returncode
            await asyncio.sleep(_ADOPTED_POLL_INTERVAL)


@dataclass
class ScreamState:
    """The watchdog's memory across ticks. `healthy` starts True (an OPTIMISTIC baseline, not
    an observation): doctrine 3 says NEVER SILENTLY DEGRADE, so if the graph is already down
    on the very first tick, that reads as a healthy→down transition and screams immediately —
    the alternative (staying silent on the first observation) is exactly the silent-degrade
    this doctrine exists to forbid."""

    healthy: bool = True
    down_since: datetime | None = None
    last_event: dict[str, Any] | None = None


ProbeFn = Callable[[], Awaitable[bool]]
NotifierFn = Callable[[str, str, str], Awaitable[None]]


async def watchdog_tick(state: ScreamState, probe: ProbeFn, notify: NotifierFn) -> bool:
    """One watchdog beat (§4.8, ruling `2ceb7ba0`): probe, and on a healthy<->down TRANSITION
    (never on a repeat) scream or all-clear. Edge-triggered, not level-triggered — a graph that
    stays down for an hour screams exactly once, the moment it fails, not once per tick. A pure
    function of (state, probe, notify) so the scream logic is testable without a watchdog loop,
    real systemd, or real DBus around it."""
    healthy = await probe()
    now = datetime.now(UTC)
    if healthy and not state.healthy:
        outage = (now - state.down_since).total_seconds() if state.down_since else None
        await notify(
            "normal", "Osiris graph reachable again",
            f"outage lasted {outage:.0f}s" if outage is not None else "outage cleared")
        state.last_event = {"kind": "recovered", "at": now.isoformat(), "outage_seconds": outage}
        state.down_since = None
    elif not healthy and state.healthy:
        state.down_since = now
        await notify(
            "critical", "OSIRIS GRAPH UNREACHABLE",
            "Postgres/Redis unreachable — the fleet is lobotomized until this clears")
        state.last_event = {"kind": "down", "at": now.isoformat(), "outage_seconds": None}
    state.healthy = healthy
    return healthy


def _make_default_notify(runner: ProcessRunner) -> NotifierFn:
    """`notify-send --urgency=<urgency>` (subprocess, bounded, awaited) — FAIL-OPEN if it is
    absent or errors: the desktop popup is a nicety, the CRITICAL log line is the guarantee
    (doctrine 3: never silently degrade). Every call logs regardless of whether the desktop
    notification actually landed."""

    async def notify(urgency: str, summary: str, body: str) -> None:
        sent = False
        if shutil.which("notify-send") is not None:
            try:
                proc = await runner(
                    ["notify-send", f"--urgency={urgency}", summary, body],
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                await asyncio.wait_for(proc.wait(), timeout=_NOTIFY_TIMEOUT)
                sent = proc.returncode == 0
            except (OSError, TimeoutError):
                sent = False
        suffix = "" if sent else " (no desktop notify)"
        if urgency == "critical":
            _log.critical("SCREAM%s: %s — %s", suffix, summary, body)
        else:
            _log.info("all-clear%s: %s — %s", suffix, summary, body)

    return notify


class Manager:
    """The daemon's brain: adoption registry + control socket + watchdog, all wired through one
    injectable `ProcessRunner` so tests never touch real systemd or DBus. `pool`/`redis` are
    optional — a `probe` passed explicitly (as every test does) needs neither."""

    def __init__(
        self, *, socket_path: Path, pool: asyncpg.Pool | None = None,
        redis: aioredis.Redis | None = None, receipts_dir: Path | None = None,
        runner: ProcessRunner = _default_runner, probe: ProbeFn | None = None,
        notify: NotifierFn | None = None,
        watchdog_interval: float = _DEFAULT_WATCHDOG_INTERVAL,
        stop_timeout: float = _DEFAULT_STOP_TIMEOUT,
        systemctl_timeout: float = _DEFAULT_SYSTEMCTL_TIMEOUT,
        cgroup_root: Path = _CGROUP_ROOT,
    ) -> None:
        self._socket_path = socket_path
        self._pool = pool
        self._redis = redis
        # Read lazily at construction (not baked into a default argument) — the same idiom
        # `bodies.LocalProvider` uses so tests can monkeypatch the module global and never
        # write into the operator's real home.
        self._receipts_dir = receipts_dir if receipts_dir is not None else BODY_RECEIPTS
        self._cgroup_root = cgroup_root
        self._runner = runner
        self._probe = probe or self._default_probe
        self._notify = notify or _make_default_notify(runner)
        self._watchdog_interval = watchdog_interval
        self._stop_timeout = stop_timeout
        self._systemctl_timeout = systemctl_timeout
        self._provider = LocalProvider(
            runner=runner, cgroup_root=cgroup_root, receipts_dir=self._receipts_dir)
        # unit-name/state, keyed by handle — the VISIBILITY registry ({"op":"bodies"} reads
        # this directly); the metering/dissolve state itself lives inside `self._provider`.
        self._registry: dict[str, dict[str, str]] = {}
        # tmux-with-a-graph's "tmux" half (§4.5): one broker for the whole daemon, holding
        # every named PTY session across however many faces attach/detach. Construction is
        # side-effect-free (no fd, no child, no event-loop registration happens until
        # `spawn()`), so it is safe to build here unconditionally, same as `self._provider`.
        self._broker = PtyBroker()
        self._scream = ScreamState()
        self._started_at = datetime.now(UTC)
        self._server: asyncio.Server | None = None
        self._watchdog_task: asyncio.Task[None] | None = None

    async def _default_probe(self) -> bool:
        """THE GRAPH's health: Postgres `SELECT 1` + Redis `PING`, each bounded to
        `_PROBE_TIMEOUT`. With no pool/redis wired at all (a bare Manager — every unit test that
        injects its own `probe` never reaches this), reads healthy rather than fabricating a
        down signal nothing actually observed."""
        if self._pool is None or self._redis is None:
            return True
        pg_ok = redis_ok = False
        try:
            async with asyncio.timeout(_PROBE_TIMEOUT):
                await self._pool.fetchval("SELECT 1")
            pg_ok = True
        except Exception:  # noqa: BLE001 - any failure here means "down", never a crash
            pg_ok = False
        try:
            async with asyncio.timeout(_PROBE_TIMEOUT):
                await self._redis.ping()
            redis_ok = True
        except Exception:  # noqa: BLE001
            redis_ok = False
        if not (pg_ok and redis_ok):
            _log.warning("graph probe: postgres=%s redis=%s", pg_ok, redis_ok)
        return pg_ok and redis_ok

    # --- boot: rebuild the world from systemd + receipts (never trust memory) ---

    async def _list_body_units(self) -> list[dict[str, Any]]:
        try:
            proc = await self._runner(
                ["systemctl", "--user", "list-units", "osiris-body-*", "--all", "--output=json"],
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=self._systemctl_timeout)
        except (OSError, TimeoutError) as exc:
            _log.warning("list-units failed, starting with an empty registry: %r", exc)
            return []
        try:
            parsed = json.loads(out.decode() or "[]")
        except ValueError:
            _log.warning("list-units returned unparseable JSON, starting with an empty registry")
            return []
        return parsed if isinstance(parsed, list) else []

    async def _resolve_cgroup(self, unit: str) -> Path | None:
        proc = await self._runner(
            ["systemctl", "--user", "show", "-p", "ControlGroup", "--value", f"{unit}.scope"],
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await proc.communicate()
        rel = out.decode().strip()
        return (self._cgroup_root / rel.lstrip("/")) if rel else None

    async def _adopt_unit(self, handle: str, unit: str, state: str) -> None:
        """Seed `LocalProvider._live` for a body this daemon never summoned. `ram_envelope_bytes`
        / `repo_ref` / `seat_anchor` / `budget_usd` have no durable manifest to recover them from
        in v1 (the graph-manifest resurrection is §4.4, a later wave) — honest placeholders, per
        the same law `bodies.py` already lives by: an honest zero beats a fabricated number."""
        cgroup = await self._resolve_cgroup(unit)
        proc = _AdoptedProc(unit, self._runner, timeout=self._stop_timeout)
        self._provider.adopt(
            handle, unit=unit, kind="adopted", repo_ref="unknown",
            seat_anchor=f"adopted:{unit}", budget_usd=None, ram_envelope_bytes=0,
            started_at=datetime.now(UTC), proc=proc, cgroup=cgroup)
        self._registry[handle] = {"unit": unit, "state": state}

    async def rebuild_world(self) -> None:
        """STATE IS RECONSTRUCTED, NEVER TRUSTED FROM MEMORY (doctrine 3). Cross-references
        live `osiris-body-*` units against the receipts dir: a handle WITH a receipt already
        dissolved (nothing to do); a handle with none is a body that outlived a dead daemon —
        RE-ADOPTED, never presumed dead. One honest line, always, even when both are zero."""
        units = await self._list_body_units()
        adopted = 0
        reaped = 0
        for u in units:
            name = str(u.get("unit") or "")
            if not (name.startswith("osiris-body-") and name.endswith(".scope")):
                continue
            handle = name[len("osiris-body-"):-len(".scope")]
            if (self._receipts_dir / f"{handle}.json").exists():
                reaped += 1  # a receipt exists: already dissolved, nothing to adopt
                continue
            await self._adopt_unit(handle, name, str(u.get("active") or "unknown"))
            adopted += 1
        _log.info("world rebuilt: adopted %d, reaped-receipts %d", adopted, reaped)

    # --- the control socket ---

    async def _start_socket(self) -> None:
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(FileNotFoundError):
            self._socket_path.unlink()  # a stale socket from a killed-not-cleaned-up prior run
        self._server = await asyncio.start_unix_server(
            self._handle_client, path=str(self._socket_path))
        os.chmod(self._socket_path, 0o600)  # ruling d6403d34: nothing on TCP, and this is a lock

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                # "pty_attach" is the ONE op that does not answer with a single JSON line and
                # move on — a successful attach SWITCHES this connection into the raw framed
                # pty stream (§4.5) for as long as the peer stays attached, so it is peeled off
                # before the generic line-in/JSON-out dispatch below ever sees it.
                try:
                    maybe_attach = json.loads(line)
                except ValueError:
                    maybe_attach = None
                if isinstance(maybe_attach, dict) and maybe_attach.get("op") == "pty_attach":
                    if await self._op_pty_attach(maybe_attach, reader, writer):
                        return  # the connection now belongs to serve_attached; nothing left
                    continue  # an {"error": ...} line was already written; stay in control mode
                response = await self._handle_line(line)
                writer.write((json.dumps(response) + "\n").encode())
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    async def _handle_line(self, line: bytes) -> dict[str, Any]:
        try:
            req = json.loads(line)
        except ValueError:
            return {"error": "invalid JSON"}
        if not isinstance(req, dict):
            return {"error": "request must be a JSON object"}
        op = req.get("op")
        if op == "status":
            return self._op_status()
        if op == "bodies":
            return self._op_bodies()
        if op == "dissolve":
            return await self._op_dissolve(req.get("handle"))
        if op == "pty_spawn":
            return await self._op_pty_spawn(req)
        if op == "pty_list":
            return self._op_pty_list()
        if op == "pty_close":
            return await self._op_pty_close(req.get("name"))
        return {"error": f"unknown op: {op!r}"}

    def _op_status(self) -> dict[str, Any]:
        uptime = (datetime.now(UTC) - self._started_at).total_seconds()
        return {
            "uptime_seconds": uptime,
            "graph_healthy": self._scream.healthy,
            "adopted_bodies": len(self._registry),
            "last_scream": self._scream.last_event,
        }

    def _op_bodies(self) -> dict[str, Any]:
        return {
            "bodies": [
                {"handle": handle, "unit": info["unit"], "state": info["state"]}
                for handle, info in sorted(self._registry.items())
            ]
        }

    async def _op_dissolve(self, handle: object) -> dict[str, Any]:
        """THE ONE MUTATING OP IN v1 — via `LocalProvider.dissolve()`, exactly the Phase 0
        lane: receipt-before-return, honest fallbacks, nothing new invented here."""
        if not isinstance(handle, str) or not handle:
            return {"error": "dissolve requires a string 'handle'"}
        if handle not in self._registry:
            return {"error": f"unknown handle: {handle}"}
        await self._provider.dissolve(handle)
        self._registry.pop(handle, None)
        return {"dissolved": handle}

    # --- the PTY broker (§4.5): tmux-with-a-graph, the daemon's half of the wire-up ---

    async def _op_pty_spawn(self, req: dict[str, Any]) -> dict[str, Any]:
        """{"op": "pty_spawn", "name": .., "argv": [..], "cwd": .., "env": {..}} — `cwd`/`env`
        are optional. Name collisions surface as `SessionExistsError` from the broker itself
        (never a silent replace); every other validation failure here is this op refusing
        loudly on a malformed request, before a single fd or child is ever created."""
        name = req.get("name")
        if not isinstance(name, str) or not name:
            return {"error": "pty_spawn requires a string 'name'"}
        argv = req.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
            return {"error": "pty_spawn requires a non-empty list of string 'argv'"}
        cwd = req.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            return {"error": "'cwd' must be a string if given"}
        env = req.get("env")
        if env is not None and not (
            isinstance(env, dict)
            and all(isinstance(k, str) and isinstance(v, str) for k, v in env.items())
        ):
            return {"error": "'env' must be a dict of string to string if given"}
        # IDENTITY AT BIRTH (spec §4.2, ruling 5cef856b): a spawn that names a seat gets the
        # Seat resolved/minted and a ONE-TIME attach token exported into the child's
        # environment before its first breath — the whisper presents them, the server binds.
        # Refused loudly BEFORE any fd or child exists: a body summoned for a seat that
        # cannot be minted must not be born unbound.
        seat_req = req.get("seat")
        seated: dict[str, Any] | None = None
        if seat_req is not None:
            if not (isinstance(seat_req, dict)
                    and isinstance(seat_req.get("handle"), str) and seat_req["handle"]):
                return {"error": "'seat' must be {'handle': str, 'house': str?} if given"}
            house = seat_req.get("house")
            if house is not None and not isinstance(house, str):
                return {"error": "'seat.house' must be a string if given"}
            if self._pool is None:
                return {"error": "seat attach requires graph access and this daemon has no "
                                 "pool — spawn without 'seat', or wire DATABASE_URL"}
            from src.actions.core import Actions
            from src.orchestrator.seats import ensure_seat, mint_attach_token
            seated = await ensure_seat(Actions(self._pool), house=house,
                                       handle=seat_req["handle"], anchor_cwd=cwd,
                                       source="osiris-manager")
            if seated.get("error"):
                return {"error": f"seat refused: {seated['error']}"}
            token = await mint_attach_token(self._pool, seat_id=seated["seat_id"],
                                            minted_by="osiris-manager")
            # base on the daemon's environ when none was given — the broker only applies
            # that default for env=None, and a child with ONLY the two seat vars has no PATH
            env = dict(env) if env is not None else dict(os.environ)
            env["OSIRIS_SEAT_ID"] = seated["seat_id"]
            env["OSIRIS_ATTACH_TOKEN"] = token
        try:
            await self._broker.spawn(name, argv, cwd=cwd, env=env)
        except SessionExistsError as exc:
            return {"error": str(exc)}
        out: dict[str, Any] = {"spawned": name}
        if seated is not None:
            out["seat_id"] = seated["seat_id"]
        return out

    def _op_pty_list(self) -> dict[str, Any]:
        return {
            "sessions": [
                {"name": info.name, "alive": info.alive, "rows": info.rows, "cols": info.cols,
                 "attach_count": info.attach_count}
                for info in self._broker.list()
            ]
        }

    async def _op_pty_close(self, name: object) -> dict[str, Any]:
        if not isinstance(name, str) or not name:
            return {"error": "pty_close requires a string 'name'"}
        if self._broker.get(name) is None:
            return {"error": f"unknown pty session: {name}"}
        await self._broker.close(name)
        return {"closed": name}

    async def _op_pty_attach(
        self, req: dict[str, Any], reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> bool:
        """{"op": "pty_attach", "name": .., "rows": .., "cols": ..} — on success, acks with
        `{"attached": name}` and then hands the connection to `serve_attached` (the SAME
        framing `serve_session` uses: O/X/I/R frames, `read_frame`/`pack_frame` — nothing
        reinvented here), which owns it until the peer disconnects. Returns True in that case
        (the caller must stop treating this connection as line-oriented JSON). An unknown or
        missing name answers with one `{"error": ...}` line instead and returns False — loud,
        never silent, and the connection stays in ordinary control mode."""
        name = req.get("name")
        if not isinstance(name, str) or not name:
            await self._write_json(writer, {"error": "pty_attach requires a string 'name'"})
            return False
        session = self._broker.get(name)
        if session is None:
            await self._write_json(writer, {"error": f"unknown pty session: {name}"})
            return False
        rows_raw, cols_raw = req.get("rows"), req.get("cols")
        rows = cols = None
        with contextlib.suppress(TypeError, ValueError):
            if rows_raw is not None and cols_raw is not None:
                rows, cols = int(rows_raw), int(cols_raw)
        await self._write_json(writer, {"attached": name})
        await serve_attached(session, reader, writer, rows=rows, cols=cols)
        return True

    async def _write_json(self, writer: asyncio.StreamWriter, response: dict[str, Any]) -> None:
        writer.write((json.dumps(response) + "\n").encode())
        await writer.drain()

    # --- the watchdog (§4.8) ---

    async def _watchdog_loop(self) -> None:
        while True:
            try:
                await watchdog_tick(self._scream, self._probe, self._notify)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a probe/notify hiccup must not kill the watchdog
                _log.exception("manager: watchdog tick crashed")
            await asyncio.sleep(self._watchdog_interval)

    # --- lifecycle ---

    async def start(self) -> None:
        await self.rebuild_world()
        await self._start_socket()
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    async def close(self) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        with contextlib.suppress(FileNotFoundError):
            self._socket_path.unlink()

    async def run_forever(self) -> None:
        """Start, then block until SIGTERM/SIGINT (systemd's `Restart=always` stop/restart
        signal) — cleanup runs in `finally`, but doctrine 3 does not depend on it: a SIGKILL
        that skips this entirely leaves the scopes and the receipts dir exactly as reachable
        to the NEXT daemon's `rebuild_world` as a clean shutdown would."""
        await self.start()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop_event.set)
        try:
            await stop_event.wait()
        finally:
            await self.close()


async def _amain() -> None:  # pragma: no cover - process entrypoint
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = get_settings()
    pool = await create_pool(settings.database_url)
    redis = create_redis(settings.redis_url)
    manager = Manager(socket_path=default_socket_path(), pool=pool, redis=redis)
    try:
        await manager.run_forever()
    finally:
        await pool.close()
        await redis.aclose()


def main() -> None:  # pragma: no cover - process entrypoint
    asyncio.run(_amain())


if __name__ == "__main__":  # pragma: no cover
    main()
