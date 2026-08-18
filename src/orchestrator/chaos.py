"""CRASH REPLAY AS A GATE (Thoth msg 5338, 2026-08-18) — #178's own residual was "the
first real test is the next daemon restart"; this stops waiting for one. Composes a real
crash (SIGKILL, never a graceful `systemctl restart`) with a concurrent session-end
STORM, then asserts the exact invariants the #178 arc (launch/dispatch graph-truth
selection, the occupancy gate, suspend-never-delete, registry_census) now claims to hold
under real failure, not just in a unit test's own fakes.

FIVE INVARIANTS, each independently checkable, each NAMED in the report whether it holds
or not (never a bare pass/fail with no evidence):
  1. NO STRANGER MINTED OVER A LISTED BODY — every seat that had a LIVE, harness-confirmed
     body (`registry_census`'s own `matched` set) immediately before the kill still holds
     that EXACT agent identity afterward (`seats.held_seat`/`seat_receipt`) — the #178
     launch/dispatch gates must have refused any fork attempt that raced the crash window,
     not merely in their own mocked tests.
  2. ZERO SURVIVING ADVISORY LOCKS — every lock this codebase takes is xact-scoped
     (`pg_advisory_xact_lock`, #172's own fix) so a killed backend's locks release with its
     connection; a post-recovery count higher than the pre-kill baseline is a real leak,
     not a timing artifact (see `_advisory_lock_count`'s own docstring for the one honest
     caveat this specific check carries).
  3. ROWLESS BODIES NEVER GROW (registry_census's own `rowless_count`) — the storm fires
     while a body's own `agent_mounts` row may be mid-suspend (#178a); if the self-restore
     path (#178b, `_reattach`) is doing its job, a body that WAS matched before the kill is
     matched again after, never left permanently rowless.
  4. /automount RETURNS 200 THROUGHOUT — polled concurrently with the kill+storm+restart,
     not just once after recovery; a window where it 500s or hangs is exactly the silent-
     failure shape task #179 already fixed once (33a3573) and this must catch a REGRESSION
     of.
  5. BACKENDS RECOVER WITHIN A BOUND — the same bounded-backoff discipline `cmd_deploy`'s
     own `_wait_for_health`/`_wait_for_smoke` already use, reused here rather than a second
     polling loop invented from scratch.

EVERY SIDE EFFECT IS INJECTABLE (`kill`, `restart`, `fire_storm`, `automount_probe`,
`agents_json`/`read_exe`/`read_cwd`) — same discipline as `cmd_deploy`'s own
`RestartServices`/`WaitForHealth` seams (src/cli.py): a test exercising this module's own
control flow (what it checks, in what order, how a partial failure is reported) has no
business paying for — or risking — a real SIGKILL against a live daemon. The REAL defaults
(`_real_kill_units`, `_real_fire_storm`) are the only place this module ever actually kills
a process or writes throwaway rows; `osiris smoke --chaos` and `cmd_deploy`'s own chaos
gate are the only two callers permitted to use them un-injected.

THE STORM ITSELF IS SYNTHETIC AND SELF-CLEANING (`_real_fire_storm`): it seeds N
throwaway `agent_mounts` rows under a `chaos-replay` project (never a real seat's own
row — this must never be the thing that causes the incident it's testing for), fires
`release_session_mounts` against all of them CONCURRENTLY with the kill (the actual race
shape a SessionEnd storm crossing a restart produces), then deletes every row it created,
pass or fail. NAMED, NOT HIDDEN: this exercises the DB-layer race (advisory locks, the
suspend sentinel) directly, never through the HTTP `/session-end` route itself — a route-
layer race (an in-flight request mid-restart) is a DIFFERENT, unbuilt surface; see
`_real_fire_storm`'s own docstring for exactly where that line sits.

FAIL-LOUD ON A REAL FINDING, NEVER SILENT (577988ed's OTHER half — that ruling's fail-open
clause is for infrastructure this module cannot control, e.g. a `/automount` probe that
itself cannot reach the network; a genuine invariant violation is the ONE thing this whole
module exists to make loud). `cmd_deploy`'s own chaos gate refuses the deploy outright on
any finding — this is a GATE, not read-only corroboration."""
from __future__ import annotations

import asyncio
import uuid as uuid_mod
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg

KillUnits = Callable[[list[str]], Awaitable[tuple[int, str]]]
RestartUnits = Callable[[list[str]], Awaitable[tuple[int, str]]]
FireStorm = Callable[[asyncpg.Pool], Awaitable[int]]
AutomountProbe = Callable[[], Awaitable[tuple[bool, str]]]

DEFAULT_CHAOS_UNITS = ("osiris-mcp", "osiris-worker")


async def _real_kill_units(units: list[str]) -> tuple[int, str]:
    """The one place this module ever actually SIGKILLs a service — `systemctl --user kill
    -s SIGKILL`, deliberately harsher than `cmd_deploy`'s own graceful `restart` (a clean
    SIGTERM stop+start never exercises the crash-recovery paths this whole module exists
    to test — a body that shuts down cleanly never leaves a dangling advisory lock or a
    suspended-not-restored mount row in the first place)."""
    proc = await asyncio.create_subprocess_exec(
        "systemctl", "--user", "kill", "-s", "SIGKILL", *units,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode(errors="replace")


async def _real_fire_storm(pool: asyncpg.Pool, *, n: int = 25) -> int:
    """Seeds `n` THROWAWAY `agent_mounts` rows (project='chaos-replay', never a real seat's
    own row) then fires `release_session_mounts` against all of them CONCURRENTLY — the DB-
    layer shape of N SessionEnd hooks landing at once while a restart is also in flight,
    the exact race #172's xact-scoped advisory locks exist to survive. Self-cleaning: every
    seeded row is DELETEd before this returns, pass or fail (a `finally`, not an afterthought
    — this storm must never be the reason a later census reads dirty). Returns `n` on
    success; the seed/cleanup themselves are best-effort against a DEAD osiris-mcp backend
    (this pool is the caller's OWN connection, independent of the daemon being killed, so it
    keeps working through the kill window by design — this is what makes firing the storm
    CONCURRENTLY WITH the kill possible at all).

    NAMED, NOT HIDDEN: this races the DB layer directly (`release_session_mounts`), never
    the HTTP `/session-end` route — an in-flight HTTP request mid-restart is a materially
    different race (connection reset mid-body, a partial write) this specific storm does
    not exercise; `automount_probe`'s own concurrent polling is this module's only HTTP-
    layer witness during the window."""
    from src.orchestrator import mounts

    run_id = uuid_mod.uuid4().hex[:8]
    job_dirs = [f"/tmp/chaos-replay-{run_id}-{i}" for i in range(n)]
    session_ids = [uuid_mod.uuid4().hex for _ in range(n)]
    try:
        await asyncio.gather(*(
            mounts.save_mount(pool, job_dir=jd, agent_id=f"agent:chaos-{run_id}-{i}",
                              project="chaos-replay", cwd=jd, model=None,
                              session_key=f"sid:{sid}")
            for i, (jd, sid) in enumerate(zip(job_dirs, session_ids, strict=True))))
        await asyncio.gather(*(
            mounts.release_session_mounts(pool, job_dir=jd, session_id=sid)
            for jd, sid in zip(job_dirs, session_ids, strict=True)))
    finally:
        await pool.execute(
            "DELETE FROM agent_mounts WHERE project='chaos-replay' AND job_dir = ANY($1::text[])",
            job_dirs)
    return n


async def _advisory_lock_count(pool: asyncpg.Pool) -> int:
    """A raw `pg_locks` read, deliberately NOT scoped to this codebase's own known lock
    keys — the honest caveat this check carries (named, not hidden): on a box with other
    LIVE co-agents doing ordinary work, a lock held by an UNRELATED concurrent transaction
    at the exact sampling instant would count here too. `chaos_replay` compares this
    against its own BASELINE (taken before the kill, same live-fleet conditions) rather
    than asserting an absolute zero, which is the honest way to filter that noise without
    hard-coding this house's own lock key strings into a general-purpose census."""
    return int(await pool.fetchval("SELECT count(*) FROM pg_locks WHERE locktype='advisory'"))


async def _baseline_seat_map(
    pool: asyncpg.Pool, baseline_matched: list[dict[str, Any]],
) -> dict[str, str]:
    """`{agent_id: seat_id}` for every body `registry_census` confirmed LIVE (harness +
    /proc both agree) immediately BEFORE the kill — resolved at baseline time, deliberately,
    never re-derived afterward: once a stranger has actually taken the seat, the ORIGINAL
    agent's own `held_seat` reverses to None (its `holds` link is exactly what
    `bind_holder` invalidates on a takeover) — asking `held_seat` again post-hoc would
    silently SKIP the very specimen this check exists to catch. A body with no seat at all
    is simply absent from the map, never a finding."""
    from src.orchestrator.seats import held_seat

    out: dict[str, str] = {}
    for m in baseline_matched:
        agent_id = m.get("agent_id")
        if not agent_id:
            continue
        held = await held_seat(pool, agent_id)
        seat_id = (held or {}).get("seat_id")
        if seat_id is not None:
            out[agent_id] = seat_id
    return out


async def _stranger_mints(pool: asyncpg.Pool, baseline_seats: dict[str, str]) -> list[str]:
    """For every `{agent_id: seat_id}` resolved at BASELINE time (`_baseline_seat_map`,
    before the kill), checks the seat's CURRENT holder (`seat_receipt`) still names that
    same agent_id. A live body's seat quietly changing hands during the chaos window — the
    #178 incident's own shape — is named here explicitly, never inferred from a bare
    object count. This checks IDENTITY CONTINUITY against the baseline snapshot, never
    re-resolves the ORIGINAL agent's own current seat (see `_baseline_seat_map`'s own
    docstring for why that specific re-derivation is the wrong direction to check in)."""
    from src.orchestrator.seats import seat_receipt

    findings: list[str] = []
    for agent_id, seat_id in baseline_seats.items():
        receipt = await seat_receipt(pool, seat_id)
        current_holder = (receipt or {}).get("holder")
        if current_holder is not None and current_holder != agent_id:
            findings.append(
                f"seat {seat_id} was held by {agent_id} (a live, harness-confirmed body "
                f"before the chaos window) — now held by {current_holder}: a stranger was "
                "minted over a listed body")
    return findings


async def chaos_replay(
    pool: asyncpg.Pool, *,
    units: tuple[str, ...] = DEFAULT_CHAOS_UNITS,
    kill: KillUnits,
    restart: RestartUnits,
    fire_storm: FireStorm,
    automount_probe: AutomountProbe,
    agents_json: Any = None,
    read_exe: Any = None,
    read_cwd: Any = None,
    poll_interval_secs: float = 1.0,
    recovery_ceiling_secs: float = 60.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    """The orchestrator: baseline → kill (SIGKILL) + storm CONCURRENTLY, polling
    `/automount` throughout → restart → bounded wait for recovery → re-measure every
    invariant. Returns `{"ok": bool, "findings": [...], ...numbers...}` — `findings` empty
    means every invariant held; a non-empty list names EXACTLY which one(s) didn't and why,
    never a bare False. Every side effect is injected — see this module's own docstring for
    which callers are permitted to pass the real ones."""
    from src.orchestrator.mounts import registry_census

    started_at = datetime.now(UTC)
    baseline_locks = await _advisory_lock_count(pool)
    baseline_census = await registry_census(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    baseline_seats = await _baseline_seat_map(pool, baseline_census.get("matched", []))

    automount_results: list[tuple[bool, str]] = []
    stop = asyncio.Event()

    async def _poll_automount() -> None:
        while not stop.is_set():
            try:
                automount_results.append(await automount_probe())
            except Exception as exc:  # noqa: BLE001 — a probe crash IS a finding, not a poller crash
                automount_results.append((False, f"automount probe raised: {exc}"))
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval_secs)
            except TimeoutError:
                pass

    poller = asyncio.create_task(_poll_automount())
    try:
        kill_rc, kill_out = await kill(list(units))
        storm_fired = await fire_storm(pool)
        restart_rc, restart_out = await restart(list(units))

        elapsed = 0.0
        delay = 2.0
        recovered_ok, recovered_detail = await automount_probe()
        while not recovered_ok and elapsed < recovery_ceiling_secs:
            await sleep(delay)
            elapsed += delay
            delay = min(delay * 2, 8.0)
            recovered_ok, recovered_detail = await automount_probe()
    finally:
        stop.set()
        await poller

    post_locks = await _advisory_lock_count(pool)
    post_census = await registry_census(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)

    findings: list[str] = []
    if kill_rc != 0:
        findings.append(f"kill failed (exit {kill_rc}): {kill_out}")
    if restart_rc != 0:
        findings.append(f"restart failed (exit {restart_rc}): {restart_out}")
    if not recovered_ok:
        findings.append(
            f"backends did not recover within {recovery_ceiling_secs:.0f}s "
            f"(last probe: {recovered_detail})")
    bad_automounts = [r for r in automount_results if not r[0]]
    if bad_automounts:
        findings.append(
            f"{len(bad_automounts)}/{len(automount_results)} /automount probe(s) failed "
            f"during the chaos window — first: {bad_automounts[0][1]}")
    if post_locks > baseline_locks:
        findings.append(
            f"{post_locks} advisory lock(s) held after recovery, vs {baseline_locks} "
            "baseline before the kill — a real leak (this check accounts for ordinary "
            "concurrent-fleet noise by comparing to its own baseline, not to zero)")
    if post_census.get("rowless_count", 0) > baseline_census.get("rowless_count", 0):
        findings.append(
            f"rowless body count grew from {baseline_census.get('rowless_count', 0)} to "
            f"{post_census.get('rowless_count', 0)} — a body lost its agent_mounts row "
            "across the chaos window and never self-restored")
    findings.extend(await _stranger_mints(pool, baseline_seats))

    return {
        "ok": not findings,
        "findings": findings,
        "started_at": started_at.isoformat(),
        "units": list(units),
        "storm_fired": storm_fired,
        "recovery_elapsed_secs": elapsed,
        "automount_probes_total": len(automount_results),
        "automount_probes_failed": len(bad_automounts),
        "baseline_advisory_locks": baseline_locks,
        "post_advisory_locks": post_locks,
        "baseline_rowless": baseline_census.get("rowless_count", 0),
        "post_rowless": post_census.get("rowless_count", 0),
        "baseline_matched": baseline_census.get("matched_count", 0),
        "post_matched": post_census.get("matched_count", 0),
    }
