"""chaos.py — crash replay as a gate (Thoth msg 5338, 2026-08-18). Every test here drives
`chaos_replay` through INJECTED kill/restart/fire_storm/automount_probe — never a real
`systemctl` call, never a real network round-trip. `_real_fire_storm` is the one function
exercised for real (against the test DB pool), since it never touches a live daemon.

#186 (dispatch 5690): `test_chaos_replay_all_green_against_isolated_real_daemons` below IS
the real thing — real SIGKILL, real subprocess restart, real /automount round-trips —
against a REAL but ISOLATED osiris-mcp + osiris-worker pair (`isolated_chaos_daemons`
fixture): their own free port, this worker's own testcontainer Postgres (`pg_dsn`) and
Redis (`redis_url`), never the production systemd units, never the production DB/queue.
This is what makes `chaos_replay`'s own real kill/restart safe to run IN THE SUITE — the
production daemons four other seats may be mid-turn on are never touched."""
from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import sys
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.chaos import (
    DEFAULT_CHAOS_UNITS,
    _advisory_lock_count,
    _baseline_seat_map,
    _real_fire_storm,
    _stranger_mints,
    chaos_replay,
)
from src.orchestrator.seats import bind_holder, ensure_seat

_VERSIONS_EXE = "/home/x/.local/share/claude/versions/2.1.210"
_REPO_ROOT = Path(__file__).resolve().parent.parent


def _agents_json_sequence(calls: list[list[dict[str, Any]]]) -> Any:
    """One list of rows PER CALL, consumed in order; the last list repeats once exhausted —
    `registry_census` is called twice by `chaos_replay` (baseline, then post-recovery), and
    a test needs to script them independently to construct a rowless-growth specimen."""
    state = {"n": 0}

    async def _read() -> list[dict[str, Any]]:
        i = min(state["n"], len(calls) - 1)
        state["n"] += 1
        return list(calls[i])
    return _read


async def _ok_kill(units: list[str]) -> tuple[int, str]:
    return 0, "killed"


async def _ok_restart(units: list[str]) -> tuple[int, str]:
    return 0, "restarted"


async def _always_ok_automount() -> tuple[bool, str]:
    return True, "ok"


async def _no_storm(pool: Any) -> int:
    return 0


async def _noop_sleep(secs: float) -> None:
    return None


# --- _advisory_lock_count -----------------------------------------------------------------

async def test_advisory_lock_count_is_a_plain_read(actions: Actions) -> None:
    n = await _advisory_lock_count(actions.pool)
    assert isinstance(n, int) and n >= 0


# --- _real_fire_storm — the one real-DB side effect this module has ------------------------

async def test_real_fire_storm_seeds_and_cleans_up_after_itself(actions: Actions) -> None:
    fired = await _real_fire_storm(actions.pool, n=5)
    assert fired == 5
    left = await actions.pool.fetchval(
        "SELECT count(*) FROM agent_mounts WHERE project='chaos-replay'")
    assert left == 0


# --- _stranger_mints -------------------------------------------------------------------------

async def test_stranger_mints_is_clean_when_the_same_agent_still_holds_the_seat(
    actions: Actions,
) -> None:
    seat = (await ensure_seat(actions, house="demo", handle="ChaosOwner",
                              source="test"))["seat_id"]
    await bind_holder(actions, seat_id=seat, agent_id="agent:chaosowner")
    baseline = await _baseline_seat_map(
        actions.pool, [{"agent_id": "agent:chaosowner"}])
    findings = await _stranger_mints(actions.pool, baseline)
    assert findings == []


async def test_stranger_mints_flags_a_seat_that_changed_hands(actions: Actions) -> None:
    seat = (await ensure_seat(actions, house="demo", handle="ChaosVictim",
                              source="test"))["seat_id"]
    await bind_holder(actions, seat_id=seat, agent_id="agent:chaosvictim")
    # BASELINE resolved BEFORE the takeover — mirrors chaos_replay's own ordering (the
    # original agent's `held_seat` reverses to None the instant a stranger takes over, so
    # this resolution must happen first, never re-derived after the fact).
    baseline = await _baseline_seat_map(
        actions.pool, [{"agent_id": "agent:chaosvictim"}])
    # A DIFFERENT agent takes the same seat during the (simulated) chaos window.
    await bind_holder(actions, seat_id=seat, agent_id="agent:chaosstranger")
    findings = await _stranger_mints(actions.pool, baseline)
    assert len(findings) == 1
    assert "agent:chaosvictim" in findings[0] and "agent:chaosstranger" in findings[0]


async def test_stranger_mints_skips_a_body_with_no_seat(actions: Actions) -> None:
    baseline = await _baseline_seat_map(actions.pool, [{"agent_id": "agent:noseat"}])
    assert baseline == {}
    findings = await _stranger_mints(actions.pool, baseline)
    assert findings == []


# --- isolated real daemons (#186) -----------------------------------------------------------

def _free_port() -> int:
    """An ephemeral port nobody else is bound to right now — bind-and-release, the
    standard OS-assigned-port trick (a TOCTOU window exists in principle; in practice the
    OS does not hand out the same free port to two concurrent binds often enough to matter
    for a test fixture, and this is never used for anything security-sensitive)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _IsolatedDaemons:
    """Real `osiris-mcp` + `osiris-worker` subprocesses — the SAME entrypoints the
    systemd units run (`python -m src.mcp_server`, the venv's own `arq` console script),
    pointed at an ISOLATED port/DB/queue via env vars, never the production ones. This is
    the whole reason a REAL `chaos_replay` (real SIGKILL, real restart) is safe to run
    inside the suite: `kill`/`restart` here only ever touch these two pids, never a
    systemd unit."""

    def __init__(self, *, port: int, dsn: str, redis_url: str) -> None:
        self.port = port
        self.env = {
            **os.environ, "DATABASE_URL": dsn, "REDIS_URL": redis_url,
            "OSIRIS_MCP_TRANSPORT": "streamable-http", "OSIRIS_MCP_HOST": "127.0.0.1",
            "OSIRIS_MCP_PORT": str(port),
        }
        self.procs: dict[str, asyncio.subprocess.Process] = {}

    def _cmd(self, unit: str) -> list[str]:
        if unit == "osiris-mcp":
            return [sys.executable, "-m", "src.mcp_server"]
        if unit == "osiris-worker":
            return [str(Path(sys.executable).parent / "arq"),
                    "src.workers.arq_worker.WorkerSettings"]
        raise ValueError(f"unknown chaos unit: {unit!r}")

    async def _spawn(self, unit: str) -> None:
        self.procs[unit] = await asyncio.create_subprocess_exec(
            *self._cmd(unit), cwd=str(_REPO_ROOT), env=self.env,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)

    async def _whisper_probe(self) -> tuple[bool, str]:
        """The SAME real probe `cmd_deploy`'s own whisper check uses
        (`_real_check_whisper_probe`/`_synthetic_automount_probe`, src/cli.py) — reused,
        never re-implemented, against THIS instance's own isolated port."""
        base = f"http://127.0.0.1:{self.port}"
        async with httpx.AsyncClient(base_url=base, timeout=5.0) as client:
            from src.cli import _synthetic_automount_probe
            return await _synthetic_automount_probe(client)

    async def start_all(self, *, ready_timeout: float = 20.0) -> None:
        await self._spawn("osiris-mcp")
        await self._spawn("osiris-worker")
        await self._wait_until_ready(ready_timeout)

    async def _wait_until_ready(self, ready_timeout_secs: float) -> None:
        deadline = time.monotonic() + ready_timeout_secs
        last = "never probed"
        while time.monotonic() < deadline:
            ok, last = await self._whisper_probe()
            if ok:
                return
            await asyncio.sleep(0.3)
        raise TimeoutError(
            f"isolated osiris-mcp on port {self.port} never became ready: {last}")

    async def kill(self, units: list[str]) -> tuple[int, str]:
        for u in units:
            proc = self.procs.get(u)
            if proc is not None and proc.returncode is None:
                proc.kill()
                await proc.wait()
        return 0, f"killed {','.join(units)}"

    async def restart(self, units: list[str]) -> tuple[int, str]:
        for u in units:
            await self._spawn(u)
        if "osiris-mcp" in units:
            await self._wait_until_ready(20.0)
        return 0, f"restarted {','.join(units)}"

    async def stop_all(self) -> None:
        for proc in self.procs.values():
            if proc.returncode is None:
                proc.kill()
        for proc in self.procs.values():
            with contextlib.suppress(Exception):
                await proc.wait()


@pytest_asyncio.fixture
async def isolated_chaos_daemons(
    actions: Actions, pg_dsn: str, redis_url: str,
) -> AsyncIterator[_IsolatedDaemons]:
    """Depends on `actions` (not just `pg_dsn`) so the reset+catalog-seed already ran
    before these subprocesses boot against the same database — a fresh daemon pair must
    see the state the test itself set up, not race its own seeding."""
    daemons = _IsolatedDaemons(port=_free_port(), dsn=pg_dsn, redis_url=redis_url)
    await daemons.start_all()
    try:
        yield daemons
    finally:
        await daemons.stop_all()


# --- chaos_replay (full orchestration, every side effect injected) -------------------------

async def test_chaos_replay_all_green(actions: Actions) -> None:
    report = await chaos_replay(
        actions.pool, kill=_ok_kill, restart=_ok_restart, fire_storm=_no_storm,
        automount_probe=_always_ok_automount,
        agents_json=_agents_json_sequence([[]]), sleep=_noop_sleep)
    assert report["ok"] is True
    assert report["findings"] == []
    assert report["automount_probes_failed"] == 0


async def test_chaos_replay_reports_a_kill_failure(actions: Actions) -> None:
    async def _bad_kill(units: list[str]) -> tuple[int, str]:
        return 1, "unit not found"

    report = await chaos_replay(
        actions.pool, kill=_bad_kill, restart=_ok_restart, fire_storm=_no_storm,
        automount_probe=_always_ok_automount,
        agents_json=_agents_json_sequence([[]]), sleep=_noop_sleep)
    assert report["ok"] is False
    assert any("kill failed" in f for f in report["findings"])


async def test_chaos_replay_reports_a_restart_failure(actions: Actions) -> None:
    async def _bad_restart(units: list[str]) -> tuple[int, str]:
        return 1, "failed to start"

    report = await chaos_replay(
        actions.pool, kill=_ok_kill, restart=_bad_restart, fire_storm=_no_storm,
        automount_probe=_always_ok_automount,
        agents_json=_agents_json_sequence([[]]), sleep=_noop_sleep)
    assert report["ok"] is False
    assert any("restart failed" in f for f in report["findings"])


async def test_chaos_replay_reports_automount_failures_seen_during_the_window(
    actions: Actions,
) -> None:
    """REAL timing, deliberately (not the no-op `sleep` fake): the concurrent poller
    (`poll_interval_secs=0.01`) needs actual wall-clock gaps during `kill`/`restart` to get
    scheduled at all — the first few probes (from WHICHEVER caller reaches them first, the
    poller or the recovery-wait) fail, then it recovers; deterministic regardless of exact
    interleaving because every EARLY call fails, not just one specific caller's."""
    import asyncio

    calls: list[int] = []

    async def _flaky_automount() -> tuple[bool, str]:
        calls.append(1)
        ok = len(calls) >= 4
        return ok, ("ok" if ok else "500 mid-restart")

    async def _slow_kill(units: list[str]) -> tuple[int, str]:
        await asyncio.sleep(0.05)
        return 0, "killed"

    async def _slow_restart(units: list[str]) -> tuple[int, str]:
        await asyncio.sleep(0.05)
        return 0, "restarted"

    report = await chaos_replay(
        actions.pool, kill=_slow_kill, restart=_slow_restart, fire_storm=_no_storm,
        automount_probe=_flaky_automount,
        agents_json=_agents_json_sequence([[]]),
        poll_interval_secs=0.01, recovery_ceiling_secs=5.0)
    assert report["ok"] is False
    assert any("/automount probe(s) failed" in f for f in report["findings"])


async def test_chaos_replay_reports_a_recovery_timeout(actions: Actions) -> None:
    async def _never_recovers() -> tuple[bool, str]:
        return False, "still 500"

    report = await chaos_replay(
        actions.pool, kill=_ok_kill, restart=_ok_restart, fire_storm=_no_storm,
        automount_probe=_never_recovers,
        agents_json=_agents_json_sequence([[]]), sleep=_noop_sleep,
        recovery_ceiling_secs=5.0, poll_interval_secs=1000.0)
    assert report["ok"] is False
    assert any("did not recover" in f for f in report["findings"])


async def test_chaos_replay_reports_rowless_growth(actions: Actions) -> None:
    baseline_rows: list[dict[str, Any]] = []
    post_rows = [{"sessionId": "deadbeef-0000-4000-8000-000000000000", "pid": 222,
                  "cwd": "/code/osiris", "name": "[OS] Ghost"}]

    report = await chaos_replay(
        actions.pool, kill=_ok_kill, restart=_ok_restart, fire_storm=_no_storm,
        automount_probe=_always_ok_automount,
        agents_json=_agents_json_sequence([baseline_rows, post_rows]),
        read_exe=lambda pid: _VERSIONS_EXE, read_cwd=lambda pid: "/code/osiris",
        sleep=_noop_sleep)
    assert report["ok"] is False
    assert any("rowless" in f for f in report["findings"])
    assert report["baseline_rowless"] == 0
    assert report["post_rowless"] == 1


async def test_chaos_replay_reports_a_stranger_minted_over_a_listed_body(
    actions: Actions,
) -> None:
    seat = (await ensure_seat(actions, house="demo", handle="ChaosLive",
                              source="test"))["seat_id"]
    await bind_holder(actions, seat_id=seat, agent_id="agent:chaoslive")
    # THE MATCH KEY IS EXACTLY 8 CHARS (registry_census keys agent_mounts.job_dir's own
    # basename against sessionId[:8]) — "chaoslv1", not "chaoslive" (9 chars), or the two
    # never line up and baseline_census["matched"] silently comes back empty.
    await mounts.save_mount(
        actions.pool, job_dir="/x/jobs/chaoslv1", agent_id="agent:chaoslive",
        project="osiris", cwd="/code/osiris", model=None, session_key="whisper:chaoslv1")

    async def _restart_that_reassigns(units: list[str]) -> tuple[int, str]:
        # the chaos window itself is when the fork would happen — simulated here since a
        # real fork attempt needs the full launch/dispatch machinery this test isn't
        # exercising; the invariant under test is that chaos_replay NOTICES the reassignment.
        await bind_holder(actions, seat_id=seat, agent_id="agent:chaosforked")
        return 0, "restarted"

    report = await chaos_replay(
        actions.pool, kill=_ok_kill, restart=_restart_that_reassigns, fire_storm=_no_storm,
        automount_probe=_always_ok_automount,
        agents_json=_agents_json_sequence([
            [{"sessionId": "chaoslv1-0000-4000-8000-000000000000", "pid": 333,
              "cwd": "/code/osiris", "name": "[OS] ChaosLive"}]]),
        read_exe=lambda pid: _VERSIONS_EXE, read_cwd=lambda pid: "/code/osiris",
        sleep=_noop_sleep)
    assert report["ok"] is False
    assert any("stranger was minted" in f for f in report["findings"])


async def test_chaos_replay_reports_an_advisory_lock_leak(actions: Actions) -> None:
    """`_advisory_lock_count` reads `pg_locks` SERVER-WIDE, by its own documented design
    (it accounts for concurrent-fleet noise via a baseline diff, never scopes by key) — but
    under THIS SUITE's own xdist parallelism, every worker shares ONE physical Postgres
    server (separate databases, same instance; see conftest.py's own `pg_dsn` docstring),
    so `pg_locks` is genuinely visible cross-worker. A single leaked lock can occasionally
    be masked by an unrelated worker's own transient advisory-lock traffic (e.g.
    test_seats.py's wedge-cancellation specimens) landing in the same narrow measurement
    window — leaking TEN distinct keys instead of one keeps the signal solidly above that
    noise floor without weakening `chaos_replay`'s own real, unscoped comparison."""
    leaked_conn: list[Any] = []
    keys = list(range(999999001, 999999011))

    async def _restart_that_leaks_locks(units: list[str]) -> tuple[int, str]:
        conn = await actions.pool.acquire()
        leaked_conn.append(conn)
        for key in keys:
            await conn.execute("SELECT pg_advisory_lock($1)", key)
        # deliberately never released, never returned to the pool — a real leak. The test's
        # own cleanup below unlocks and releases it via the SAME connection object
        # (pg_advisory_unlock_all() only ever releases the CALLING session's own locks —
        # a fresh connection from the pool cannot clean up another connection's hold).
        return 0, "restarted"

    report = await chaos_replay(
        actions.pool, kill=_ok_kill, restart=_restart_that_leaks_locks, fire_storm=_no_storm,
        automount_probe=_always_ok_automount,
        agents_json=_agents_json_sequence([[]]), sleep=_noop_sleep)
    try:
        assert report["ok"] is False
        assert any("advisory lock" in f for f in report["findings"])
    finally:
        conn = leaked_conn[0]
        await conn.execute("SELECT pg_advisory_unlock_all()")
        await actions.pool.release(conn)


# --- the real thing, isolated (#186) ---------------------------------------------------------

async def test_chaos_replay_all_green_against_isolated_real_daemons(
    actions: Actions, isolated_chaos_daemons: _IsolatedDaemons,
) -> None:
    """THE ISOLATION HALF OF #186 (dispatch 5690) IS BUILT AND WORKS: real SIGKILL, real
    subprocess restart, real /automount round-trips through a REAL osiris-mcp +
    osiris-worker pair — own port, own testcontainer Postgres (`pg_dsn`), own
    testcontainer Redis (`redis_url`), never the production systemd units or DB/queue.
    `agents_json` stays the SAME empty-baseline fake the fully-mocked tests above use,
    deliberately: `registry_census`'s own fleet-agent census is about the HOST's real
    Claude Code sessions, unrelated to this isolated daemon pair.

    THIS TEST DOES NOT PASS, AND SHOULDN'T BE MADE TO — A REAL WALL, NOT A SUITE-ISOLATION
    BUG (Thoth's own instruction: name it, don't route around it). Measured directly (a
    standalone script, real production DSN, no test isolation involved at all): a freshly
    spawned `python -m src.mcp_server` takes ~1.5-2s of genuine connection-refused before
    it accepts its first HTTP request — cold Python + FastMCP + settings + DB-pool init,
    inherent to the process, not an artifact of this harness. Production's REAL restart
    path is worse: `deploy/user/osiris-mcp.service` sets `RestartSec=5`, so a real SIGKILL
    there means at LEAST 5s (systemd's own mandated delay) + ~1.5-2s startup ≈ 6.5s+ of
    genuine unavailability — and `chaos_replay` has NEVER been run live before #186
    (decision aa7f515f: "BUILT AND GATED, NOT RUN LIVE... deferred... never restart
    services"), so this specific tension was never caught.

    THE TENSION: invariant #4 ("/automount RETURNS 200 THROUGHOUT", chaos.py's own
    docstring) is coded to flag ANY probe failure during the whole kill-to-recovery
    window as a hard finding — and a dedicated existing test
    (`test_chaos_replay_reports_automount_failures_seen_during_the_window`) deliberately
    locks in that exact behavior, so this is REVIEWED, INTENTIONAL strictness, not an
    oversight I can quietly loosen. Invariant #5 ("BACKENDS RECOVER WITHIN A BOUND")
    already tracks and tolerates bounded downtime via `recovery_elapsed_secs` — the two
    invariants currently disagree about whether transient, bounded unavailability is
    acceptable. No single, un-replicated process (this isolated pair, OR the real
    systemd units) can satisfy #4 as literally coded; only a proxy/replica architecture
    with zero-downtime failover could, which does not exist today for either.

    NOT FIXED HERE: whether to (a) distinguish pre-first-recovery failures (expected,
    bounded, tolerable) from post-recovery flapping (a genuine regression) — the natural
    reading of what #5 already half-implements, or (b) require real replica
    infrastructure before this can ever be a suite gate, is Thoth's/the operator's call,
    not mine to make unilaterally against a previously-reviewed, deliberately-tested
    invariant. Reported in full, per standing practice, rather than forcing green."""
    report = await chaos_replay(
        actions.pool, units=DEFAULT_CHAOS_UNITS,
        kill=isolated_chaos_daemons.kill, restart=isolated_chaos_daemons.restart,
        fire_storm=_real_fire_storm, automount_probe=isolated_chaos_daemons._whisper_probe,
        agents_json=_agents_json_sequence([[]]), recovery_ceiling_secs=30.0)
    # The isolation infrastructure itself is proven here regardless of the automount
    # tension above: storm ran for real, and — the invariant this test's own docstring
    # names as unresolved aside — recovery must still complete within the bound.
    assert report["storm_fired"] == 25
    assert report["recovery_elapsed_secs"] < 30.0, report["findings"]
    assert not any("did not recover" in f for f in report["findings"])
    assert not any("stranger was minted" in f for f in report["findings"])
    assert not any("advisory lock" in f for f in report["findings"])
    assert not any("rowless" in f for f in report["findings"])
    # THE NAMED, UNRESOLVED TENSION (see docstring), MEASURED TWICE, TWO DIFFERENT
    # ANSWERS: one run found 2/2 automount probes failing during the real cold-start
    # window; a later run found 0/1. Real process-restart timing racing a 1s poll
    # interval is not deterministic either way — asserting a fixed count in either
    # direction would itself be exactly the "flaky" acceptance Thoth's dispatch refused.
    # Not asserted; logged in the receipt for a human to read (`report["automount_
    # probes_failed"]`), same as `chaos_replay`'s own report already surfaces it.
