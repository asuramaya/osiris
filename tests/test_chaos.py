"""chaos.py — crash replay as a gate (Thoth msg 5338, 2026-08-18). Every test here drives
`chaos_replay` through INJECTED kill/restart/fire_storm/automount_probe — never a real
`systemctl` call, never a real network round-trip. `_real_fire_storm` is the one function
exercised for real (against the test DB pool), since it never touches a live daemon."""
from __future__ import annotations

from typing import Any

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.chaos import (
    _advisory_lock_count,
    _baseline_seat_map,
    _real_fire_storm,
    _stranger_mints,
    chaos_replay,
)
from src.orchestrator.seats import bind_holder, ensure_seat

_VERSIONS_EXE = "/home/x/.local/share/claude/versions/2.1.210"


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
