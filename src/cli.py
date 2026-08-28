"""osiris — the operator's console-script (task #69, ruling 45b074bf, thread 16a0c76b: "no
bash runes; the user never debugs the machinery"). Thin subcommands, each wrapping an
EXISTING verb rather than re-deriving it:

  osiris attach <handle>            resolve handle -> a live PTY session, hand off to
                                     src.manager.attach (replaces the raw
                                     `.venv/bin/python -m src.manager.attach "[OS] imhotep"`
                                     the operator was handed before this build)
  osiris smoke                      the same probe src.orchestrator.smoke runs for the fleet
  osiris seed [--compositions-only] src.init's seeder (task #63's own deploy-step flag)
  osiris launch <handle> [--model]  body a seat via `claude --bg` by default (task #72,
             [--debug]              following trigger.launch_seat's own flip, rulings
                                     0fe36e59 + 33d6a2eb clause 3) — every body lands in the
                                     operator's own `claude agents` list by construction.
                                     `--debug` keeps the original osiris PTY-broker lane alive
                                     (the manager daemon directly, never trigger.py's
                                     launch_seat() — that verb is explicitly seat-to-seat
                                     only, "THE OPERATOR NEVER CALLS THIS"; a human at this
                                     CLI is a different trust boundary, the same one
                                     src.manager.attach.py already stands in for)
  osiris fleet [--full]             the same fleet() the MCP tool answers, called over the
                                     wire (never a second implementation of what it computes)
  osiris migrate [--check]          env-correct `alembic upgrade head` (thread c4681c38 leg
                                     1) — IN-PROCESS via alembic's own command API, never a
                                     subprocess rune (a bare `alembic upgrade head` connects
                                     to the prod-shaped 5432 default because env.py reads
                                     DATABASE_URL and nothing set the dev fallback first —
                                     exactly the class ruling 45b074bf bans). `--check`
                                     reports a pending revision without applying it.
  osiris deploy                     the deploy ritual as one verb (thread e51a841c): refuse
                                     on a dirty tracked src/ tree (a live near-miss shipped a
                                     half-written edit this way), compare migrations and
                                     refuse-or-run them BEFORE anything restarts (thread
                                     c4681c38 leg 2 — a deploy is atomic from the schema's
                                     point of view), restart osiris-mcp/worker/console, run
                                     smoke, and name any un-run seeder step by comparison
                                     instead of assuming one happened
  osiris merge <dupe> <into>        the same self-typing orchestrator.merge.merge the merge
             --evidence --actor     MCP tool wraps (thread 2446, renamed from fold-project
                                     per dispatch 3683 — fold_project no longer exists as an
                                     MCP tool, ruling 31c02dca/decision a926a8d0, and the CLI
                                     had silently kept the old name) — the sanctioned second
                                     door for a worker whose sandbox classifier permits an
                                     installed entrypoint but refuses a raw DATABASE_URL
                                     script, or when a client's MCP tool index is stale.
                                     `osiris fold-project` still works (identical args,
                                     SoftwareProject-only) as a hidden, printed-deprecated
                                     alias — never advertised, never silently broken.
  osiris unmerge <dupe>             the same orchestrator.merge.unmerge the unmerge MCP tool
             --because --actor      wraps — reverses a wrongful merge. Dry run by default
             [--execute]            (matches the MCP tool's own convention); built alongside
                                     merge's own CLI rename, dispatch 3683's own point that
                                     an MCP pair had no reason to stay asymmetric here.
  osiris charter-for <seat>         the same manager/operator-enforced charter.charter_for
             --repos --because      the charter_for MCP tool wraps (thread 2474) — same
             --actor                second-door reasoning as fold-project, same guard,
                                     untouched
  osiris amend-practice <ref>       the same capture.amend_practice the amend_practice MCP
             <amendment> --actor    tool wraps (thread 06c3529b) — narrows a LIVE practice's
                                     guidance without touching its id/statement/witness count.
                                     Calls the orchestrator function directly with an explicit
                                     --actor (fold-project/charter-for's own pattern, not
                                     cmd_fleet's anonymous call_mcp_tool one) because this is a
                                     WRITE that needs real attribution: an anonymous MCP session
                                     has no mounted identity, so a call_mcp_tool round-trip
                                     would stamp the amendment's source as the generic
                                     "session" bucket (mcp_server._source_for's own documented
                                     fallback) instead of a named actor — a real provenance
                                     loss for a governance-relevant write, unlike cmd_fleet's
                                     read-only round-trip where no attribution is at stake.
  osiris annotate-thread <ref>      the same capture.annotate_thread the annotate_thread MCP
             <note> --actor         tool wraps (thread 2474 — named there alongside
                                     amend_decision as sharing fold_project's stale-tool-index
                                     shape, but never built until now) — appends to a thread's
                                     record without closing it. Same explicit-actor,
                                     direct-orchestrator-call pattern as amend-practice above.
  osiris amend-decision <ref>       the same capture.amend_decision the amend_decision MCP
             <addendum> --actor     tool wraps (thread 2474, the other half of the pair named
                                     above) — appends reasoning to a LIVE decision without
                                     superseding it. Same pattern, same reason.
  osiris mint-seat <handle>         the same mintseat.mint_seat the mint_seat MCP tool wraps —
             --manager <seat>       a DIFFERENT shape of gap than the four doors above: the MCP
             [--project] [--house]  tool has no `manager` parameter at all, it infers the
             [--model] --actor      manager from the CALLING agent's own held seat, which a raw
             [--adopt] [--force]    terminal doesn't have. Takes `manager` explicitly instead —
                                     closes the "brand-new seat needs a hand-rolled python -c
                                     heredoc" gap CLI.md's own house law names as a finding.

CANONICAL ENV RESOLUTION (the actual root-fix, 3e96c10e's cousin): every DB-backed command
applies src.config.dev_env.apply_dev_fallback() first — a bare invocation must target the
SAME dev instance the systemd user units already inline, never silently fall to Settings'
prod-shaped 5432/6379 default. `attach`/`launch`'s manager-socket calls and `smoke`/`fleet`'s
MCP round-trip need no such fallback (neither touches Postgres directly); only `seed`,
`launch`'s own seat-facts lookup + honesty check, `migrate`, and `deploy`'s migration gate
do.

Every error is honest and names the next step — no raw traceback reaches the operator's
terminal for a condition this module can anticipate (a dark daemon, an unreachable database,
an ambiguous or unknown handle)."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
import sys
import textwrap
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import asyncpg

from src.config.settings import Settings
from src.manager.client import default_socket_path, manager_call

ManagerCall = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
SpawnClaudeBg = Callable[..., Awaitable[None]]
AgentsJson = Callable[..., Awaitable[list[dict[str, Any]]]]
ResumeSpawn = Callable[..., Awaitable[None]]

# dispatch 3678, the operator's own "make the cli friendly": every sanctioned-second-door
# command below used to REQUIRE --actor, forcing a human at a raw terminal to type a value
# that is always going to be the same one anyway. `console` is already a member of
# `_OPERATOR_ACTORS` (src/orchestrator/seats.py) — a raw terminal call IS a console act by
# construction (no MCP round-trip, no borrowed agent identity), so it already carries
# operator authority; defaulting to it is naming a fact, not granting one. Never inferred
# silently past this: the flag stays a real override for a caller who wants a different
# name attributed (a script driving this CLI on someone else's behalf, say).
_CONSOLE_ACTOR = "console"


async def _default_manager(req: dict[str, Any]) -> dict[str, Any]:
    """The real manager socket, bound to the daemon's own default path — the injectable
    `manager` param this module's async commands take defaults to this exact call, matching
    trigger.py's own `launch_seat(manager=...)` precedent so tests can swap a fake in without
    a live daemon, same as that verb's own test suite does."""
    return await manager_call(req, socket_path=str(default_socket_path()))


def _house_tag(house: str | None) -> str:
    """Mirrors trigger.py's own private `_house_tag` exactly (not imported: this CLI is a
    human's own hand spawning via the manager daemon directly, a deliberately different path
    from launch_seat()'s seat-to-seat lane — see cmd_launch's own docstring)."""
    h = (house or "").strip()
    return h[:2].upper() if h else "OS"


def match_session(sessions: list[dict[str, Any]], handle: str) -> tuple[str | None, list[str]]:
    """(name, candidates). `name` is set only for an unambiguous match against the manager
    daemon's own pty_list roster; `candidates` lists every session name that matched loosely,
    for an honest disambiguation message when `name` is None. A handle is matched against the
    TAIL of the window's own '[TAG] Handle' name (trigger.py's _house_tag convention) so a
    caller of this CLI never needs to know that formatting exists at all."""
    h = handle.strip().lower()
    if not h:
        return None, []

    def tail(name: str) -> str:
        return name.rsplit("] ", 1)[-1].strip().lower()

    names = [s["name"] for s in sessions if isinstance(s, dict) and isinstance(s.get("name"), str)]
    exact = [n for n in names if tail(n) == h]
    if len(exact) == 1:
        return exact[0], []
    if len(exact) > 1:
        return None, exact
    loose = [n for n in names if h in tail(n)]
    if len(loose) == 1:
        return loose[0], []
    return None, loose


def resolve_model(
    explicit: str | None, seat_intended: str | None, wake_default: str | None,
) -> str | None:
    """Launch's own model precedence: an explicit --model flag wins outright; else the target
    SEAT's own stamped intended_model; else the wake-lane economy default; else None (the
    claude CLI's own default). Thread 20e4feb6's still-open bug is trigger.py's launch() never
    consulting the middle tier at all — this CLI is new code and does not repeat it."""
    return explicit or seat_intended or wake_default or None


async def _mcp_url() -> str:
    from src.config.settings import get_settings

    s = get_settings()
    return f"http://{s.osiris_mcp_host}:{s.osiris_mcp_port}/mcp"


# --- attach ----------------------------------------------------------------------------------

async def cmd_attach(handle: str, *, manager: ManagerCall = _default_manager) -> int:
    from src.manager import attach as attach_mod

    try:
        roster = await manager({"op": "pty_list"})
    except (OSError, TimeoutError) as exc:
        print(f"osiris attach: the manager daemon is unreachable ({exc}) — is osiris-manager "
              "running? (systemctl --user status osiris-manager)", file=sys.stderr)
        return 1
    sessions = roster.get("sessions")
    sessions = sessions if isinstance(sessions, list) else []
    name, candidates = match_session(sessions, handle)
    if name is None:
        if candidates:
            print(f"osiris attach: {handle!r} matches {len(candidates)} live sessions, not "
                  f"one: {candidates}. Use a more specific handle.", file=sys.stderr)
        else:
            live = [s.get("name") for s in sessions if isinstance(s, dict)]
            print(f"osiris attach: no live session matches {handle!r}. Live sessions: "
                  f"{live or '(none)'}", file=sys.stderr)
        return 1
    return attach_mod.main([name])


# --- smoke -----------------------------------------------------------------------------------

async def _run_smoke_probes_full() -> tuple[list[str], list[str]]:
    """The two probes, composed, PLUS osiris-mcp's own non-blocking `warnings` (task
    #180 follow-through, Thoth DM 5257: the registry_census `rowless` count folded into
    smoke's verdict) — split from `_run_smoke_probes` only so the deploy gate's retry
    loop (which must key its backoff on FAILS alone, never a non-blocking warning) keeps
    its existing narrow `list[str]` contract untouched. Returns (fails, warnings)."""
    import httpx

    from src.config.settings import get_settings
    from src.orchestrator.smoke import (
        call_mcp_smoke,
        smoke_chrome,
        summarize_failures,
        summarize_warnings,
    )

    settings = get_settings()
    url = await _mcp_url()

    async def local_chrome() -> dict[str, str]:
        async with httpx.AsyncClient(
            base_url=settings.osiris_console_base_url, timeout=5.0,
        ) as client:
            return await smoke_chrome(client)

    chrome, mcp_result = await asyncio.gather(local_chrome(), call_mcp_smoke(url))
    return summarize_failures(chrome, mcp_result), summarize_warnings(mcp_result)


async def _run_smoke_probes() -> list[str]:
    """The two probes, composed — shared by `cmd_smoke` and `cmd_deploy` so neither
    re-derives it. Returns the flat failure list (empty = all green); see
    `_run_smoke_probes_full` for the non-blocking warnings alongside it."""
    fails, _warnings = await _run_smoke_probes_full()
    return fails


async def _health_probe() -> bool:
    """One GET at /health — cheap (no chrome render, no MCP round-trip) and only answers
    after the console app's own lifespan finishes standing up its pool (src/api/app.py), so
    it's an honest readiness signal, not just "uvicorn is listening". False on ANY failure
    (refused, timed out, non-200) — not up yet is not a smoke failure."""
    import httpx

    from src.config.settings import get_settings

    try:
        async with httpx.AsyncClient(
            base_url=get_settings().osiris_console_base_url, timeout=2.0,
        ) as client:
            r = await client.get("/health")
            return r.status_code == 200
    except Exception:  # noqa: BLE001 - not up yet, not a smoke failure
        return False


async def _wait_for_health(
    probe: Callable[[], Awaitable[bool]] = _health_probe, *,
    ceiling_secs: float = 120.0, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[bool, float]:
    """A BOUNDED poll of /health (never indefinite), run BEFORE smoke so a still-starting
    console reads as "still starting", not a smoke false-alarm. Measured, same box same day
    (Thoth DM 2823): console cold-start ranged 47s-94s — a fixed sleep sized to one sample
    is a lie waiting for a slower boot, so this reports the REAL elapsed wait instead of
    asserting one. Retries with backoff (1s, 2s, 4s, 8s, 8s, ...) capped at `ceiling_secs`
    (120s: comfortable margin over the measured 94s, still bounded). Returns (ready,
    elapsed) — ready=False past elapsed>=ceiling_secs is a REAL finding (the console did not
    come up), not a timing race."""
    elapsed = 0.0
    delay = 1.0
    ready = await probe()
    while not ready and elapsed < ceiling_secs:
        await sleep(delay)
        elapsed += delay
        delay = min(delay * 2, 8.0)
        ready = await probe()
    return ready, elapsed


async def _wait_for_smoke(
    probe: Callable[[], Awaitable[list[str]]] = _run_smoke_probes, *,
    ceiling_secs: float = 30.0, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[list[str], float]:
    """A BOUNDED wait-for-up loop (never indefinite): a service that just restarted (uvicorn
    still binding, the MCP pool still warming) needs a few seconds — a single immediate probe
    cried wolf on a genuinely healthy deploy (found live, batch 4's maiden `osiris deploy`
    run: all-red immediately, all-green five seconds later). Retries with backoff (2s, 4s,
    8s, 8s, ... capped at `ceiling_secs`) until the probe comes back clean or the ceiling
    elapses. Returns (fails, elapsed) — an empty `fails` past elapsed>0 means it recovered;
    a non-empty `fails` once elapsed>=ceiling_secs is a REAL finding, not a false alarm."""
    elapsed = 0.0
    delay = 2.0
    fails = await probe()
    while fails and elapsed < ceiling_secs:
        await sleep(delay)
        elapsed += delay
        delay = min(delay * 2, 8.0)
        fails = await probe()
    return fails, elapsed


def diff_tool_lists(before: dict[str, str], after: dict[str, str]) -> list[str]:
    """Named additions/removals/changes between two MCP tool-list snapshots (thread 6a78e64b
    leg 2) — pure, so the exact wording is testable without a live server. `+name` a tool the
    after-list has that the before-list didn't; `-name removed` the reverse; `~name changed`
    the same name with a different fingerprint (a signature or docstring edit) — so a deploy
    names exactly which verbs are arriving, not just that something changed somewhere."""
    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = sorted(n for n in (set(before) & set(after)) if before[n] != after[n])
    return ([f"+{n}" for n in added] + [f"-{n} removed" for n in removed]
           + [f"~{n} changed" for n in changed])


async def cmd_smoke(*, chaos: bool = False) -> int:
    if chaos:
        return await cmd_smoke_chaos()
    fails, warnings = await _run_smoke_probes_full()
    if not fails:
        print("smoke: all green (8 chrome routes + the live mcp pool)")
    else:
        print("SMOKE FAILURES:")
        for f in fails:
            print(" -", f)
    for w in warnings:
        print("WARNING:", w)
    return 0 if not fails else 1


CHAOS_LEDGER_KEY = "chaos-replay:last"


async def _real_chaos_gate(pool: asyncpg.Pool) -> dict[str, Any]:
    """The real wiring `osiris smoke --chaos` and `cmd_deploy`'s own chaos gate share — the
    only place either caller passes `chaos_replay` its real, un-injected side effects.
    `automount_probe` reuses `_real_check_whisper_probe` unchanged (the same throwaway
    /automount+/session-end round trip `cmd_deploy`'s ordinary whisper check already makes,
    just polled repeatedly here instead of once)."""
    from src.orchestrator.chaos import (
        DEFAULT_CHAOS_UNITS,
        _real_fire_storm,
        _real_kill_units,
        chaos_replay,
    )

    return await chaos_replay(
        pool, units=DEFAULT_CHAOS_UNITS, kill=_real_kill_units,
        restart=_real_restart_services, fire_storm=_real_fire_storm,
        automount_probe=_real_check_whisper_probe)


async def _real_full_suite_gate(repo_root: Path) -> dict[str, Any]:
    """The real wiring `osiris deploy`'s own full-suite gate uses (task #186, Thoth DM
    5637): `pytest -q -n 4` against `repo_root` — the SAME bounded worker cap
    `scripts/gate_hook.py`'s own scoped runs use, never `-n auto`, so this gate cannot
    itself become the thing that exhausts a host already running concurrent agents'
    commits. Each invocation spins its OWN throwaway pg testcontainer (pytest's own
    session fixture), entirely separate from the real dev Postgres `osiris deploy`
    operates against — the concurrency risk this must respect (#100) is host resource
    contention with OTHER pytest runs, not a shared database. `osiris deploy` itself is a
    single coordinated action (the operator's/coordinator's own, never run by multiple
    fleet agents at once — the same assumption `record_deploy`'s own cursor write already
    makes), so this gate's own worst case is bounded the same way gate_hook.py's is.

    Bounded at 600s (the suite measured ~200-230s under real load tonight; generous
    headroom, never unbounded) — a timeout is reported as a genuine failure, never
    swallowed as fail-open (577988ed's fail-open clause is for infrastructure this can't
    control; a suite that cannot even finish is exactly the invariant violation this gate
    exists to catch, same posture as the chaos gate beside it)."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "pytest", "-q", "-n", "4",
        cwd=repo_root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        out_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=600)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        return {"ok": False, "summary": "pytest timed out after 600s — never finished",
               "returncode": None}
    out = out_bytes.decode(errors="replace")
    tail = "\n".join(out.strip().splitlines()[-15:])
    return {"ok": proc.returncode == 0, "summary": tail, "returncode": proc.returncode}


async def cmd_smoke_chaos(*, pool: asyncpg.Pool | None = None) -> int:
    """`osiris smoke --chaos` — runs the crash replay standalone (never as a side effect of
    an ordinary `osiris smoke`) and records the numbers to the deploy ledger's own cursor
    store (`CHAOS_LEDGER_KEY`) whether it passes or fails, so `cmd_deploy`'s own gate (and
    a human reading the ledger later) never has to re-derive them from scrollback."""
    import json

    from src.orchestrator.monitor import set_cursor

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:smoke-chaos")
        except Exception as exc:  # noqa: BLE001
            print(f"osiris smoke --chaos: could not reach postgres — {exc}", file=sys.stderr)
            return 1
    try:
        report = await _real_chaos_gate(pool)
        await set_cursor(pool, CHAOS_LEDGER_KEY, json.dumps(report))
        if report["ok"]:
            print(f"chaos replay: all invariants held — {report['storm_fired']} session-end(s) "
                  f"fired concurrently with the kill, recovered in "
                  f"{report['recovery_elapsed_secs']:.0f}s, "
                  f"{report['automount_probes_total']} /automount probe(s) during the window "
                  f"all 200")
        else:
            print("CHAOS REPLAY FINDINGS:")
            for f in report["findings"]:
                print(" -", f)
        return 0 if report["ok"] else 1
    finally:
        if owns_pool:
            await pool.close()


# --- boot-status -------------------------------------------------------------------------------

async def cmd_boot_status(*, pool: asyncpg.Pool | None = None) -> int:
    """Report-only rollout check (thread 0e5bae06, #84) — names every active seat NOT
    carrying a compiled managed section, classified by why, same shape as
    `composition_gap_notes`: a build isn't done when its acceptance test passes, it's
    done when the effect reaches every office, and 'reached most of them' is a gap this
    prints by name, never a count that can read clean while 19 offices are silently
    unreached."""
    from src.orchestrator.boot_compiler import boot_rollout_gap_notes, boot_rollout_gaps

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:boot-status")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris boot-status: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        gaps = await boot_rollout_gaps(pool)
    finally:
        if owns_pool:
            await pool.close()
    if not gaps:
        print("boot: every active seat carries a compiled managed section")
        return 0
    for note in boot_rollout_gap_notes(gaps):
        print(note)
    return 1


# --- seed ------------------------------------------------------------------------------------

async def cmd_seed(*, compositions_only: bool, pool: asyncpg.Pool | None = None) -> int:
    from src.actions.core import Actions
    from src.init import _print_next_steps, init

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:seed")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris seed: could not reach postgres at {settings.database_url} — {exc}. "
                  "Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        result = await init(Actions(pool), canon=not compositions_only)
    finally:
        if owns_pool:
            await pool.close()
    _print_next_steps(result)
    return 0


# --- launch ----------------------------------------------------------------------------------

async def _await_launch_confirmation(
    pool: asyncpg.Pool, manager: ManagerCall, *, spawned_name: str, anchor_cwd: str,
    tries: int = 8, interval: float = 1.0,
) -> tuple[bool, str | None]:
    """A short, BOUNDED poll (never an indefinite block) for two honest facts: is the window
    alive, and has a fresh body actually mounted at the office and self-reported a model. This
    is the exact by-hand check decision 8e9c48d9 did for Imhotep's own respawn, built in so a
    launch's own receipt says it up front instead of a human excavating it after the fact."""
    alive = False
    mounted_model: str | None = None
    for _ in range(tries):
        if not alive:
            try:
                cur = await manager({"op": "pty_list"})
            except (OSError, TimeoutError):
                cur = {}
            sessions = cur.get("sessions")
            if isinstance(sessions, list) and any(
                isinstance(s, dict) and s.get("name") == spawned_name and s.get("alive")
                for s in sessions
            ):
                alive = True
        if mounted_model is None:
            row = await pool.fetchrow(
                "SELECT model FROM agent_mounts WHERE cwd=$1 AND "
                "last_seen > now() - interval '30 seconds' ORDER BY last_seen DESC LIMIT 1",
                anchor_cwd)
            if row is not None:
                mounted_model = row["model"] or ""
        if alive and mounted_model is not None:
            break
        await asyncio.sleep(interval)
    return alive, mounted_model


async def _resolve_launch_target(pool: asyncpg.Pool, handle: str) -> dict[str, Any] | None:
    """Handle -> seat facts (with `seat_id` folded in), or None with an honest stderr message
    already printed. Shared by both launch lanes below — the seat lookup and its error cases
    don't change with the substrate, only what happens once a target is found."""
    from src.orchestrator.seats import seat_facts, seats_by_handle

    seat_ids = await seats_by_handle(pool, handle)
    if not seat_ids:
        print(f"osiris launch: no living Seat holds handle {handle!r}.", file=sys.stderr)
        return None
    if len(seat_ids) > 1:
        print(f"osiris launch: {handle!r} is ambiguous — {len(seat_ids)} seats share it: "
              f"{seat_ids}. Use a more specific handle.", file=sys.stderr)
        return None
    facts = await seat_facts(pool, seat_ids[0])
    if not facts["handle"]:
        print(f"osiris launch: {seat_ids[0]} carries no handle assertion — a body cannot "
              "be named for a nameless seat.", file=sys.stderr)
        return None
    if not facts["anchor_cwd"]:
        print(f"osiris launch: {facts['handle']} ({seat_ids[0]}) has no anchor_cwd — "
              "establish_office first; a body needs a room to be born in.", file=sys.stderr)
        return None
    facts["seat_id"] = seat_ids[0]
    return facts


def _collapse_resume_log(log: list[str]) -> str:
    """#153 (Thoth msg 3802, live specimen: `osiris launch metron` printing seven
    near-identical refusal clauses for four distinct sessions): `_lineage_resume_
    candidate` reports one entry PER GENERATION it walks, but the distinguishing fact
    is per-SESSION — a lineage that compacted repeatedly inside one session reports the
    identical verdict once per generation. Collapse RUNS of adjacent entries whose text
    past their leading `gen N` is byte-identical (== the same session, same verdict)
    into one `gens N-M (...) (kx)` line, then rank every entry that DIDN'T collapse —
    a resumable hop, a crossed-registry refusal, anything genuinely distinct — ABOVE the
    collapsed repeats, so the one line that actually matters is never buried under a
    wall of near-duplicate prose ('a wall of near-duplicate prose IS a rune', #135)."""
    gen_re = re.compile(r"^gen (\S+)(.*)$")
    groups: list[tuple[list[str], str]] = []
    for entry in log:
        m = gen_re.match(entry)
        if m is None:
            groups.append(([], entry))
            continue
        gen, rest = m.group(1), m.group(2)
        if groups and groups[-1][0] and groups[-1][1] == rest:
            groups[-1][0].append(gen)
        else:
            groups.append(([gen], rest))
    singles: list[str] = []
    repeats: list[str] = []
    for gens, rest in groups:
        if not gens:
            singles.append(rest)
        elif len(gens) == 1:
            singles.append(f"gen {gens[0]}{rest}")
        else:
            try:
                lo, hi = sorted((gens[0], gens[-1]), key=int)
            except ValueError:
                lo, hi = gens[-1], gens[0]
            repeats.append(f"gens {lo}-{hi}{rest} ({len(gens)}x)")
    return "; ".join(singles + repeats)


async def _cmd_launch_harness(
    handle: str, *, model: str | None, pool: asyncpg.Pool, wake_default: str | None,
    spawn: SpawnClaudeBg, agents_json: AgentsJson, resume_spawn: ResumeSpawn,
    settings: Settings | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    """THE DEFAULT LANE (task #72, following trigger.launch_seat's own flip, rulings 0fe36e59
    + 33d6a2eb clause 3): `claude --bg` + `claude agents --json`, the harness's own front-end
    surface — every body this creates is visible in the operator's own `claude agents` list
    BY CONSTRUCTION. Mirrors launch_seat's harness lane exactly (same spawn/agents_json
    primitives, same boot-prompt wording via `_bg_boot_prompt` — one wording, never two to
    drift apart) but skips its managed_by/caller-seat gate: launch_seat's own docstring is
    explicit that the operator is a different trust boundary and never calls it directly —
    this function IS that boundary, same as the PTY lane below it.

    TREE_CWD (task #135/#136, 2026-08-03, ruling 983ec87a — two doors onto one act must
    return the same receipt): this door had drifted from launch_seat's own #103 update —
    hardcoded to `office` and never reading `tree_cwd` at all, so it could not correctly
    body any tree-bound seat; it always spawned into the office, silently. Now mirrors
    launch_seat's own tree_cwd handling exactly: bound-but-missing refuses (osiris never
    provisions a tree), unset falls back to `office` unchanged.

    THE RESUME LANE (task #136, 2026-08-05, ruling via decision 536de12f + Thoth msg 3732 —
    "GO — #136 LANE SWITCH"): before minting fresh, mirrors launch_seat's own already-proven
    resume branch exactly — `_lineage_resume_candidate` + `_resume_guard` + `resume_spawn`
    (trigger.py's `-p --resume` lane), reused verbatim, never a third bespoke implementation.
    VISIBILITY RIDES OSIRIS'S OWN REGISTRY, NEVER `claude agents --json`, ON PURPOSE (decision
    536de12f, confirming a829a15d): a resumed `-p --resume` body cannot appear in that roster
    even when it explicitly calls mount() mid-turn — proven live, twice, independently — that
    registry only ever lists sessions spawned with `--bg --name`, unrelated to osiris's own
    bookkeeping by construction. So a successful resume here returns immediately, WITHOUT
    polling `agents_json` for it — polling would just spin out to the same false "not yet
    visible" message on every resume, every time, which is worse than not polling at all.

    THE CONFESSION KEEPS FIRING, UNCHANGED, ON THE FALLBACK PATH ONLY (Thoth's explicit
    requirement: "the confession path you just fixed must keep working... do not let the
    lane switch quietly bypass or double it"): a successful resume never reaches
    `dormant_history_confession` at all (nothing left to confess — the launch already acted
    on it); every path that falls through to a fresh spawn still gets it, exactly as before
    this lane existed. The resume attempt's own reason (`resume_log`) prints separately,
    named every time win or lose — Thoth's own rule for launch_seat's identical receipt
    ("a correct decision made silently is indistinguishable from a broken one").

    THE UNKNOWN ARM NEVER MINTS A STRANGER (thread ef88e2bb, operator, 2026-08-17, ruling
    7d6815bb): a `resident-unknown` gate — an ABSENCE of signed testimony, not a positive
    finding of a different mind — used to fall through to the same fresh `--bg` mint as a
    genuine `crossed-registry` finding, exactly the bug that spawned strangers over
    ferryman's and halcyon's real, resumable heads. Now it refuses the WHOLE launch instead,
    spawning nothing and naming the exact `claude -p --resume <sid>` a human can run to
    confirm the session themselves. `crossed-registry` (a positively different mind) still
    falls through to fresh — that session was never this seat's, so a fresh body under its
    name is legitimate."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.seats import seat_receipt
    from src.orchestrator.trigger import (
        _DM_RESUME_PROMPT,
        _bg_boot_prompt,
        _launch_twin_check,
        _lineage_resume_candidate,
        _resume_guard,
        _tree_exists,
    )

    facts = await _resolve_launch_target(pool, handle)
    if facts is None:
        return 1
    office = facts["anchor_cwd"]
    tree_cwd = facts["tree_cwd"]
    launch_cwd = office
    if tree_cwd:
        if not _tree_exists(tree_cwd):
            print(f"osiris launch: {handle!r} names tree_cwd={tree_cwd!r} but it does not "
                  "exist on disk — osiris expects the harness (or a human, via "
                  "EnterWorktree) to have created it before launch; it never provisions "
                  "one itself.", file=sys.stderr)
            return 1
        launch_cwd = tree_cwd

    # THE SHARED TWIN GUARD (task #148's contested seam 4, ruling 983ec87a "two doors, one
    # receipt"): reads BOTH claude agents --json (the harness's own, known-incomplete roster
    # — invisible to a resumed non-bg body by construction) AND agent_mounts (osiris's own
    # registry, which a resumed body's mid-turn mount() call DOES reach), same helper
    # launch_seat's own harness-native lane calls, so the two doors can never drift.
    twin = await _launch_twin_check(pool, agents_json, launch_cwd)
    if twin is not None:
        seen_via = [s for s in (
            f"claude agents --json ({twin['harness'].get('name')!r})"
            if twin["harness"] else None,
            f"agent_mounts ({twin['mounts']['agent_id']}, last_seen "
            f"{twin['mounts']['last_seen']})" if twin["mounts"] else None,
        ) if s]
        # ALREADY-LIVE IS THE GOAL STATE, NOT A REFUSAL — exit 0, and say so in success
        # language. Diagnosed live 2026-08-28 (operator: "osiris launch complains about a
        # lot of things"): this was the ONLY outcome in this function printing a
        # refusal-shaped line to STDOUT and returning 0, while its two siblings
        # (missing tree_cwd, resident-unknown) both print to stderr and return 1. So it
        # read as a complaint to a human AND as a plain success to a script, which is the
        # #151 disease — one channel that cannot distinguish what actually happened.
        #
        # THE LAW, symmetric with `osiris stop`: each verb exits 0 when the world is
        # already in the state it was asked for. launch => a body exists. stop => none
        # does. Neither is a failure and neither should shout. The DIAGNOSTIC (how we
        # know) goes to stderr so `osiris launch X | ...` stays clean; the VERDICT stays
        # on stdout, and now names which of the two things happened.
        print(f"already-live: {handle} — a body is already there, nothing started")
        print(f"osiris launch: seen via {', '.join(seen_via)}", file=sys.stderr)
        return 0

    resolved_model = resolve_model(model, facts["intended_model"], wake_default)

    from src.config.settings import get_settings
    st = settings or get_settings()
    holder = ((await seat_receipt(pool, facts["seat_id"])) or {}).get("holder")
    resume_outcome = await _lineage_resume_candidate(
        pool, holder, st, repo=launch_cwd) if holder else ["no seat holder on record"]
    resume_log = resume_outcome[1] if isinstance(resume_outcome, tuple) else resume_outcome
    resume = resume_outcome[0] if isinstance(resume_outcome, tuple) else None
    if resume is not None:
        # holder is truthy whenever resume is set — resume_outcome only comes from
        # _lineage_resume_candidate(holder, ...), never the bare-string branch, when
        # holder was falsy. Asserted, not silently narrowed: a violated invariant here
        # should be loud, never a quiet skip of the identity gate.
        assert holder is not None
        # hop count (#173a, mirrored from launch_seat's own identical wiring — ruling
        # 983ec87a, two doors must return the same receipt): the SAME arithmetic
        # _lineage_resume_candidate's own success line renders ("...resumable, N hop(s)
        # back") — its log always ends with exactly one success entry when `resume` is
        # set, so the count of entries BEFORE it is N.
        gate, refusal = await _resume_guard(
            pool, resume, _generation(holder)[0], seat_id=facts["seat_id"], st=st,
            hop=len(resume_log) - 1, launch_cwd=launch_cwd)
        if gate == "resident-unknown":
            # THE FIX FOR ef88e2bb (operator, 2026-08-17, ruling 7d6815bb) — mirrors
            # launch_seat's own fix exactly (ruling 983ec87a, two doors one receipt): an
            # ABSENCE of signed testimony is not evidence this head belongs to someone
            # else. "crossed-registry" (a POSITIVE finding) still falls through to a
            # fresh mint below; "resident-unknown" refuses the WHOLE launch instead —
            # nothing spawned, the exact resume command named. NOTE (#173a): only
            # reached when the zero-hop graph door (hop/launch_cwd above) did NOT
            # already clear the gate.
            print(f"osiris launch: REFUSING — {handle!r} has a possibly-resumable "
                  f"session {resume[0][:8]} but {refusal}. Run `claude -p --resume "
                  f"{resume[0]}` by hand to confirm it yourself; osiris will not mint "
                  "a fresh mind over a resumable head it merely couldn't verify.",
                  file=sys.stderr)
            return 1
        if gate is not None:
            resume_log = [*resume_log, f"{gate} guard refused it: {refusal}"]
            resume = None
    if resume is not None:
        resumed_session_id, resumed_repo = resume[0], resume[1]
        await resume_spawn(resumed_repo, _DM_RESUME_PROMPT, resume_session=resumed_session_id,
                           model=resolved_model, allowed_tools=st.osiris_wake_allowed_tools
                           or None)
        print(f"osiris launch: resumed session {resumed_session_id[:8]} as a ONE-SHOT turn — "
              f"walked {len(resume_log)} generation(s) back to find it "
              f"({_collapse_resume_log(resume_log)}); it runs the brief and exits; `claude "
              "agents --json` shows it only WHILE it runs, never after (a harness fact, not a bug: "
              "a further mail wake continues it, exactly like any other dormant addressee).")
        stamped_model = facts.get("intended_model")
        if stamped_model and resolved_model != stamped_model:
            print(f"  MODEL MISMATCH: spawned on {resolved_model!r} but the seat's own "
                  f"stamped intended_model is {stamped_model!r} — never silent (thread "
                  "20e4feb6).", file=sys.stderr)
        return 0

    print(f"osiris launch: {handle!r} not resumed — {_collapse_resume_log(resume_log)} (gate: "
          f"min_tail_bytes={st.osiris_resume_min_tail_bytes}, ceiling="
          f"{st.osiris_resume_ceiling_bytes}b)")

    from src.ingest.sessions import dormant_history_confession, dormant_history_note

    # BOTH SLUGS, ALWAYS (task #135/#136): office and tree_cwd are two DIFFERENT slugs by
    # design (#103) — a dormant transcript can sit under either one, so check both
    # regardless of which one this launch is actually spawning into, and confess whichever
    # is freshest. locate_transcript_by_cwd was single-slug-blind; dormant_history_confession
    # now takes every candidate cwd it's given.
    dormant = dormant_history_confession(office, *([tree_cwd] if tree_cwd else []))
    if dormant is not None:
        print(f"osiris launch: {handle!r} — {dormant_history_note(dormant)}",
              file=sys.stderr)

    name = f"[{_house_tag(facts['house'])}] {facts['handle']}"
    anchor = str(Path.home() / ".claude" / "jobs" / facts["seat_id"].replace(":", "-"))
    boot_prompt = _bg_boot_prompt(office=office, anchor=anchor, handle=facts["handle"])

    try:
        await spawn(launch_cwd, name=name, model=resolved_model, prompt=boot_prompt)
    except OSError as exc:
        print(f"osiris launch: claude --bg failed to start ({exc}) — nothing was spawned.",
              file=sys.stderr)
        return 1
    print(f"osiris launch: spawned {name!r} via claude --bg, requested model="
          f"{resolved_model or '(claude CLI default)'}")

    alive_row: dict[str, Any] | None = None
    for _ in range(8):
        try:
            alive_row = next((r for r in await agents_json(cwd=launch_cwd)
                              if isinstance(r, dict) and r.get("cwd") == launch_cwd), None)
        except (OSError, TimeoutError, ValueError):
            alive_row = None
        if alive_row is not None:
            break
        await sleep(1.0)
    if alive_row is None:
        print("  not yet visible in `claude agents --json` — it may still be booting or "
              "self-binding; re-check with `osiris fleet` in a few seconds.")
        return 0
    session_id = alive_row.get("sessionId")
    print(f"  confirmed: find it in `claude agents` as {name!r}"
          + (f" (session {session_id})" if session_id else ""))
    return 0


async def _cmd_launch_pty(
    handle: str, *, model: str | None, pool: asyncpg.Pool, manager: ManagerCall,
    wake_default: str | None,
) -> int:
    """`--debug`'s FALLBACK LANE: bodies a seat via the manager daemon DIRECTLY (pty_spawn),
    never trigger.py's launch_seat() — same trust-boundary reasoning as the harness lane
    above. Kept alive for an incident, or a build with no `claude --bg` — attachable via
    `osiris attach`, which the harness-native lane's own body is not. Reports the model it
    actually confirms mounted, honestly and within a bounded wait — never a bare
    'launched: true'."""
    facts = await _resolve_launch_target(pool, handle)
    if facts is None:
        return 1

    try:
        roster = await manager({"op": "pty_list"})
    except (OSError, TimeoutError) as exc:
        print(f"osiris launch: the manager daemon is unreachable ({exc}) — is "
              "osiris-manager running?", file=sys.stderr)
        return 1
    sessions = roster.get("sessions")
    sessions = sessions if isinstance(sessions, list) else []
    existing, _ = match_session(sessions, handle)
    if existing:
        print(f"osiris launch: a live body already holds {handle!r} — {existing!r}. Not "
              f"minting a twin (attach to it: `osiris attach {handle}`).")
        return 0

    resolved_model = resolve_model(model, facts["intended_model"], wake_default)
    argv = ["claude", *(["--model", resolved_model] if resolved_model else [])]
    name = f"[{_house_tag(facts['house'])}] {facts['handle']}"
    anchor = str(Path.home() / ".claude" / "jobs" / facts["seat_id"].replace(":", "-"))
    child_env = {k: v for k, v in os.environ.items() if k != "CLAUDE_JOB_DIR"}
    child_env["CLAUDE_JOB_DIR"] = anchor

    try:
        res = await manager(
            {"op": "pty_spawn", "name": name, "argv": argv, "cwd": facts["anchor_cwd"],
             "seat": {"handle": facts["handle"], "house": facts["house"]},
             "job_dir": anchor, "env": child_env})
    except (OSError, TimeoutError) as exc:
        print(f"osiris launch: manager unreachable mid-spawn ({exc}) — nothing confirmed "
              "spawned.", file=sys.stderr)
        return 1
    if not isinstance(res, dict) or res.get("error"):
        detail = res.get("error") if isinstance(res, dict) else str(res)
        print(f"osiris launch: spawn refused — {detail}", file=sys.stderr)
        return 1

    spawned = res.get("spawned")
    if not isinstance(spawned, str):
        print(f"osiris launch: manager accepted the spawn but named no window ({res!r}) — "
              "cannot confirm anything; check with `osiris fleet`.", file=sys.stderr)
        return 1
    print(f"osiris launch: spawned {spawned!r}, requested model="
          f"{resolved_model or '(claude CLI default)'}")
    alive, mounted_model = await _await_launch_confirmation(
        pool, manager, spawned_name=spawned, anchor_cwd=facts["anchor_cwd"])
    print(f"  window alive: {alive}" + ("" if alive else
          " (not yet — re-check with `osiris fleet` shortly; if this persists, "
          "systemctl --user status osiris-manager)"))
    if mounted_model is None:
        print("  mount not yet observed within the wait — the claude is still booting or "
              "self-binding; re-check with `osiris fleet` in a few seconds.")
    elif resolved_model and mounted_model != resolved_model:
        print(f"  MISMATCH: requested model={resolved_model!r} but the body that mounted "
              f"reports model={mounted_model!r} — this is thread 20e4feb6's own bug class "
              "(launch spawning the wrong model, silently); check the manager daemon's "
              "argv handling before assuming this launch is healthy.")
    else:
        print(f"  confirmed: a body mounted at {facts['anchor_cwd']} reporting "
              f"model={mounted_model!r}")
    return 0


async def cmd_launch(
    handle: str, *, model: str | None, pool: asyncpg.Pool | None = None,
    manager: ManagerCall = _default_manager, wake_default: str | None = None,
    debug: bool = False, spawn: SpawnClaudeBg | None = None,
    agents_json: AgentsJson | None = None, resume_spawn: ResumeSpawn | None = None,
    settings: Settings | None = None,
) -> int:
    """Bodies a seat. DEFAULT LANE (task #72): harness-native `claude --bg`, following
    trigger.launch_seat's own flip (rulings 0fe36e59 + 33d6a2eb clause 3) — every body lands
    in the operator's own `claude agents` list by construction. `debug=True` (the CLI's
    `--debug`) keeps the original osiris PTY-broker lane alive as an explicit fallback for an
    incident or a build with no `claude --bg` — attachable via `osiris attach`, which the
    default lane's body is not. `pool`/`manager`/`wake_default`/`spawn`/`agents_json`/
    `resume_spawn`/`settings` are all injectable (mirrors launch_seat's own test seam) —
    production callers (main()) leave them at their real defaults."""
    from src.orchestrator.trigger import _claude_agents_json, _spawn_claude, _spawn_claude_bg
    spawn = spawn or _spawn_claude_bg
    agents_json = agents_json or _claude_agents_json
    resume_spawn = resume_spawn or _spawn_claude

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = settings or get_settings()
        wake_default = settings.osiris_wake_model
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=2,
                application_name="osiris-cli:launch")
        except Exception as exc:  # noqa: BLE001
            print(f"osiris launch: could not reach postgres at {settings.database_url} — "
                  f"{exc}.", file=sys.stderr)
            return 1
    try:
        if debug:
            return await _cmd_launch_pty(handle, model=model, pool=pool, manager=manager,
                                         wake_default=wake_default)
        return await _cmd_launch_harness(handle, model=model, pool=pool,
                                         wake_default=wake_default, spawn=spawn,
                                         agents_json=agents_json, resume_spawn=resume_spawn,
                                         settings=settings)
    finally:
        if owns_pool:
            await pool.close()


# --- stop ------------------------------------------------------------------------------------

async def cmd_stop(handle: str, *, reason: str = "", as_json: bool = False,
                   pool: asyncpg.Pool | None = None) -> int:
    """`osiris launch`'s INVERSE, and the reason it exists: launch has had a terminal door
    since task #72 and stop had none, so a human could start a body from the shell and had
    no way to end one from the shell. Every other exit was a raw kill by hand — untracked,
    unaudited, and exactly the "dead ends and corpses" the operator named.

    Calls trigger.stop_seat DIRECTLY as caller='operator' — the same function the MCP
    `stop` tool calls, never a second implementation. The operator lane skips ONE check
    (the managed_by edge, which governs agent-to-agent authority and has nothing to say
    about the human); the seat must still resolve, still have a holder, and the body must
    still be /proc-confirmed by the same census every other door reads."""
    from src import cli_render as render
    from src.actions.core import Actions
    from src.orchestrator.trigger import stop_seat

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(settings.database_url, min_size=1, max_size=2,
                                     application_name="osiris-cli:stop")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris stop: could not reach postgres at {settings.database_url} — "
                  f"{exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        out = await stop_seat(Actions(pool), caller="operator", target=handle,
                              reason=reason)
    finally:
        if owns_pool:
            await pool.close()

    render.emit(out, as_json=as_json, title=f"stop · {handle}")
    # EXIT CODE CARRIES THE VERDICT so a script can branch on it. `no-live-body` is exit 0
    # ON PURPOSE: "there is nothing running there" is a SUCCESSFUL outcome for anyone
    # cleaning up — a teardown loop must not treat an already-dead body as a failure, or
    # every clean run ends red and nobody trusts the signal.
    status = out.get("status")
    if status in ("stopped", "no-live-body"):
        return 0
    print(f"osiris stop: {status} — {out.get('detail', '')}", file=sys.stderr)
    return 1


# --- fleet -----------------------------------------------------------------------------------

async def cmd_fleet(*, full: bool, as_json: bool = False) -> int:
    from src import cli_render as render
    from src.orchestrator.mcp_client import call_mcp_tool

    url = await _mcp_url()
    result = await call_mcp_tool(url, "fleet", {"full": full})
    if isinstance(result, str):
        print(f"osiris fleet: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    # `tree` is fleet()'s OWN pre-rendered ASCII picture — when the server sends one it is
    # already the human answer and this side must not second-guess it. --json still wins
    # over it, because a machine asked for data, not a drawing.
    tree = result.get("tree")
    if as_json:
        render.emit(result, as_json=True)
    elif isinstance(tree, str):
        print(tree)
    else:
        render.emit(result, as_json=False, title="fleet")
    return 0


# --- roster ------------------------------------------------------------------------------------

async def cmd_roster(*, repo: str | None, as_json: bool = False) -> int:
    from src import cli_render as render
    from src.orchestrator.mcp_client import call_mcp_tool

    url = await _mcp_url()
    result = await call_mcp_tool(url, "roster", {"repo": repo})
    if isinstance(result, str):
        print(f"osiris roster: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    render.emit(result, as_json=as_json, title=f"roster · {repo}" if repo else "roster")
    return 0


# --- desk / show — READING THE RECORD (thread 00913be9, Thoth's CLI-surface audit): the
# CLI shipped 22 write-shaped subcommands and zero read-shaped ones over the record itself
# (mail, threads, decisions) — a human at his own terminal could WRITE annotate-thread but
# could not read his own desk. #138's own lesson applied: both capabilities already existed
# as MCP tools (inbox(project='operator', peek=True) already IS the organized desk;
# recall(ref) already IS the untruncated single-object read) — this only NAMES them as CLI
# doors, the same call_mcp_tool + render.emit shape fleet/roster already use. Nothing new
# was built underneath; the surface decision was which two, not which seven. --------------

async def cmd_desk(*, as_json: bool = False) -> int:
    """osiris desk — the operator's own organized queue, read at a terminal instead of only
    the web console or an agent peeking on his behalf. Always a peek: reading the desk never
    leases a brief, and settling one is only ever the operator's own explicit word."""
    from src import cli_render as render
    from src.orchestrator.mcp_client import call_mcp_tool

    url = await _mcp_url()
    result = await call_mcp_tool(url, "inbox", {"project": "operator", "peek": True})
    if isinstance(result, str):
        print(f"osiris desk: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    render.emit(result, as_json=as_json, title="desk")
    return 0


async def cmd_show(ref: str, *, as_json: bool = False) -> int:
    """osiris show <ref> — the full, untruncated record for one Thread or Decision, by
    UUID, 8-char short id, or summary substring — the same recall() an agent already reads.
    Refuses loudly (never guesses) when nothing matches; exits nonzero either way a script
    can check."""
    from src import cli_render as render
    from src.orchestrator.mcp_client import call_mcp_tool

    url = await _mcp_url()
    result = await call_mcp_tool(url, "recall", {"ref": ref})
    if isinstance(result, str):
        print(f"osiris show: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    render.emit(result, as_json=as_json, title=f"show · {ref}")
    return 1 if result.get("error") else 0


# --- deploy ----------------------------------------------------------------------------------

DEPLOY_UNITS = ("osiris-mcp", "osiris-worker", "osiris-console")

GitStatus = Callable[[Path], list[tuple[str, str]]]
RestartServices = Callable[[list[str]], Awaitable[tuple[int, str]]]
InstallUserUnits = Callable[[Path], Awaitable[list[str]]]


def user_unit_sources(repo_root: Path) -> list[Path]:
    """Every dev-box systemd USER unit `osiris deploy` owns — deploy/user/*.service (thread
    e6fd3772 piece 3-infra). These were, before this, five hand-installed units this box's own
    operator diverged by hand from deploy/{osiris-mcp,osiris-worker}.service (the /opt SYSTEM
    templates, a different shape entirely: User=/EnvironmentFile=/opt paths) — nothing in git
    was ever the box's actual running config. deploy/user/ is the single source of truth now;
    a fresh name here is picked up automatically, same as `oneshot_deployed_scripts` above."""
    d = repo_root / "deploy" / "user"
    if not d.is_dir():
        return []
    return sorted(d.glob("*.service"))


async def _real_install_user_units(repo_root: Path) -> list[str]:
    """Copies deploy/user/*.service over ~/.config/systemd/user/ (creating the dir if this is
    a fresh box) and daemon-reloads ONLY if something actually changed — an idle box's every
    deploy should not spam a reload it doesn't need. The unit's own content is the source of
    truth; nothing here renders or substitutes (systemd's own %h/%u specifiers do that at
    activation time), so this is a straight byte-for-byte copy, diffed first.

    A `repo_root` with no deploy/user/ (a test's own tmp_path, or a checkout that predates
    this) touches NOTHING outside itself — no directory created, no real ~/.config read —
    rather than silently mkdir-ing into the real caller's home on every such call."""
    sources = user_unit_sources(repo_root)
    if not sources:
        return ["unit files: no deploy/user/ found — nothing to install"]
    target_dir = Path.home() / ".config" / "systemd" / "user"
    target_dir.mkdir(parents=True, exist_ok=True)
    notes: list[str] = []
    changed: list[str] = []
    for src in sources:
        dest = target_dir / src.name
        new_content = src.read_text()
        old_content = dest.read_text() if dest.exists() else None
        if old_content == new_content:
            continue
        dest.write_text(new_content)
        changed.append(src.name)
        notes.append(f"unit: {'updated' if old_content is not None else 'installed (new)'} "
                      f"{src.name}")
    if not changed:
        notes.append("unit files: unchanged, no daemon-reload needed")
        return notes
    proc = await asyncio.create_subprocess_exec(
        "systemctl", "--user", "daemon-reload",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    if proc.returncode != 0:
        notes.append(f"systemctl --user daemon-reload FAILED: {out.decode(errors='replace')}")
    else:
        notes.append(f"systemctl --user daemon-reload: ok ({', '.join(changed)})")
    return notes


def _find_repo_root(start: Path | None = None) -> Path | None:
    """`git rev-parse --show-toplevel` from `start` (default CWD) — reuses git's own worktree
    resolution rather than hand-walking for `.git`, so it works from any subdirectory of the
    checkout, not just its root. None (never a raised exception) when CWD isn't inside a git
    repo at all — deploy is inherently tied to a specific checkout, unlike the DB/daemon/MCP-
    backed subcommands above, which is why this is the one place a bare CWD dependency is
    correct rather than the bug task #69 otherwise closes."""
    import subprocess

    try:
        out = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], cwd=start, capture_output=True,
            text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    return Path(out.stdout.strip())


def _real_git_status(repo_root: Path) -> list[tuple[str, str]]:
    """(status_code, path) for every line of `git status --porcelain`, path relative to
    `repo_root`. `--porcelain` is stable, script-friendly output — not `git status`'s own
    human-formatted default."""
    import subprocess

    out = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo_root, capture_output=True, text=True,
        timeout=10, check=False)
    lines = []
    for line in out.stdout.splitlines():
        if not line:
            continue
        lines.append((line[:2], line[3:]))
    return lines


def dirty_tracked_src_files(status: list[tuple[str, str]]) -> list[str]:
    """Tracked (never `??` — a brand-new untracked file is imported by nothing yet, so it
    cannot be a half-shipped edit to already-running code) src/ files with a staged or
    unstaged modification. This is the exact shape of the near-miss the guard exists for:
    src/orchestrator/handshake.py carrying another agent's uncommitted WIP while the three
    services import straight from the working tree."""
    return sorted(path for code, path in status if path.startswith("src/") and code != "??")


def oneshot_deployed_scripts(repo_root: Path) -> dict[str, str]:
    """script path (repo-relative) -> unit name, for every `Type=oneshot` unit under deploy/
    whose ExecStart names a scripts/ file — the COMMIT-DEPLOYED class (operator ruling via
    Thoth, msg 1481): a oneshot timer reads its script fresh off disk at every fire, so
    nothing about a restart (or a hold) gates it — the commit (or even just the working
    tree, if uncommitted) IS the deploy. Derived from deploy/*.service rather than a
    hardcoded list, so a newly added oneshot unit is picked up automatically."""
    import re

    out: dict[str, str] = {}
    deploy_dir = repo_root / "deploy"
    if not deploy_dir.is_dir():
        return out
    for unit_file in sorted(deploy_dir.glob("*.service")):
        text = unit_file.read_text()
        if not re.search(r"^Type=oneshot\s*$", text, re.MULTILINE):
            continue
        m = re.search(r"^ExecStart=.*?(scripts/\S+)", text, re.MULTILINE)
        if m:
            out[m.group(1)] = unit_file.stem
    return out


def commit_deployed_notes(status: list[tuple[str, str]], oneshot: dict[str, str]) -> list[str]:
    """For every dirty (staged OR unstaged, `??` included — an uncommitted NEW oneshot script
    is just as immediately live as a modified one) path that backs a known oneshot unit, name
    it plainly: this is not gated by anything `osiris deploy` does."""
    notes = []
    for code, path in status:
        unit = oneshot.get(path)
        if unit is None:
            continue
        notes.append(f"{path} (backs oneshot timer {unit!r}, status {code.strip() or '??'}) — "
                     "read fresh from disk at every fire; NOT gated by a restart or a hold. "
                     "Whatever's there now is already effectively live — review it directly.")
    return notes


async def _real_restart_services(units: list[str]) -> tuple[int, str]:
    """The one place this module ever actually restarts a service — `systemctl --user
    restart`. Every test of the surrounding deploy logic injects a fake here instead."""
    proc = await asyncio.create_subprocess_exec(
        "systemctl", "--user", "restart", *units,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode(errors="replace")


def _alembic_config(repo_root: Path) -> Any | None:
    """The alembic.ini Config this repo's migrations use, or None when `repo_root` carries no
    alembic.ini/alembic/ at all — a repo_root that isn't this project's own checkout shape.
    Shared by `_alembic_head` (disk-only, no DB) and `_real_run_migrations` (actually applies
    them) so both read the exact same script_location."""
    from alembic.config import Config

    if not (repo_root / "alembic.ini").is_file():
        return None
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    return cfg


def _alembic_head(repo_root: Path) -> str | None:
    """The latest migration's own revision id, read off the version files on disk — no DB
    connection needed for this half of the comparison. None (never a raised CommandError)
    when `repo_root` carries no alembic.ini/alembic/ at all — a repo_root that isn't this
    project's own checkout shape is a different problem than a migration gap, and
    alembic_gap_note treats None as 'could not be determined', never as a false mismatch."""
    from alembic.script import ScriptDirectory
    from alembic.util.exc import CommandError

    cfg = _alembic_config(repo_root)
    if cfg is None:
        return None
    try:
        return ScriptDirectory.from_config(cfg).get_current_head()
    except CommandError:
        return None


def composition_gap_notes(have: set[str], expected: set[str]) -> list[str]:
    """NAME-set difference, never a count (thread a25365a9): a `db_count >= expected`
    comparison cannot fail in the direction it exists to detect, because the same table
    also holds user-saved compositions — eleven of them, measured live, alongside the 24
    defaults. Up to eleven vanished defaults would still read "up to date" under a count
    check, and it silently did, on two consecutive deploys. One note per missing default,
    naming it, so `osiris seed --compositions-only` is an instruction a reader can act on
    rather than a hope. Extra rows (user-saved or otherwise) never mask a gap here — only
    a name present in `expected` and absent from `have` counts as one."""
    return [f"compositions: default {name!r} missing from the DB — run `osiris seed` "
            "(or `osiris seed --compositions-only`)." for name in sorted(expected - have)]


def composition_drift_notes(
    live_specs: dict[str, Any], expected: dict[str, dict[str, Any]],
) -> list[str]:
    """MISSING-OR-DIFFERENT, not just missing (obligation e4612853, ruling 38c71544 — "two
    records of one truth with no reconciler": DEFAULT_COMPOSITIONS's Python constant and a
    composition's own DB row are synced ONLY by someone remembering the separate manual
    `osiris seed --compositions-only` step; nothing enforces it and, until this, nothing
    detected skipping it). A real instance: a346a0d edited PROJECT_BRIEFING's columns in
    source, the commit landed, the deploy was green, and the live 'project-briefing' row
    kept serving the pre-edit spec for hours — every instrument said success; the read was
    stale (fixed live, ruling 143899e1).

    NAME every drifted composition, never a bare count (same law composition_gap_notes
    already established, thread a25365a9) — a count can't be acted on; a name can.

    CANNOT DISTINGUISH a forgotten re-save from a DELIBERATE live hand-edit in the composer
    (both travel through the identical save_composition() write path, and the DB row carries
    no marker of which happened) — stated here rather than guessed at with a heuristic that
    would eventually cry wolf on legitimate forks and get ignored. A composition present in
    `expected` but absent from `live_specs` is composition_gap_notes' own job, silently
    skipped here to keep the two checks from double-reporting the same row."""
    out = []
    for name, source_spec in sorted(expected.items()):
        if name not in live_specs:
            continue
        if json.dumps(source_spec, sort_keys=True) != json.dumps(live_specs[name], sort_keys=True):
            out.append(
                f"compositions: {name!r} DIFFERS from its own DEFAULT_COMPOSITIONS source — "
                "either a deliberate live hand-edit (leave it) or a forgotten re-save after "
                "editing the source constant (run `osiris seed --compositions-only`); this "
                "check cannot tell which.")
    return out


def alembic_gap_note(current: str | None, head: str | None) -> str | None:
    if head is None or current == head:
        return None
    return (f"alembic: DB is at revision {current!r}, the latest migration is {head!r} — "
           "run `alembic upgrade head`.")


def _alembic_revision_known(repo_root: Path, revision: str) -> bool | None:
    """Whether `revision` exists as a script anywhere in THIS TREE's own alembic chain —
    None (undeterminable, e.g. no alembic.ini here) on any load failure, never a false
    confident answer, same fail-open discipline as `_alembic_head`. This is the disk-only
    half of the exact question decision 8d3f5e2d names: a revision the live DB carries but
    this tree's script directory has never heard of means some OTHER branch's migration ran
    against shared DATABASE_URL before merging."""
    from alembic.script import ScriptDirectory
    from alembic.util.exc import CommandError

    cfg = _alembic_config(repo_root)
    if cfg is None:
        return None
    try:
        ScriptDirectory.from_config(cfg).get_revision(revision)
        return True
    except CommandError:
        return False


def composition_room_gap_notes(unassigned: list[str]) -> list[str]:
    """NAME every composition carrying no room_id, never a count (ruling 89e67c49, the
    follow-up to task #94's own gate fix): a NULL room_id renders nowhere outside the
    rarely-visited god view — the exact defect a NULL section had until compositions.
    save_composition() closed the write path for a genuine CREATE. That fix cannot close
    every door, deliberately: room_id carries no NOT NULL constraint (unlike section) because
    the column is `REFERENCES rooms(id) ON DELETE SET NULL` — deleting a room a composition
    still points to writes this exact NULL back, at the DB level, past any Python guard or
    logger.warning. This is the backstop for THAT door: not prevention, detection — the same
    role composition_gap_notes plays for a missing default."""
    return [f"compositions: {name!r} has no room_id — invisible outside the god view; "
            "re-save it with a room (save_composition() defaults new saves to 'engineer', "
            "but only closes the gap going forward, never for a row already orphaned)."
            for name in sorted(unassigned)]


async def _composition_gaps(pool: asyncpg.Pool) -> list[str]:
    """Composition seeding only — the alembic half moved to `_apply_pending_migrations`
    (thread c4681c38 leg 2), which now runs BEFORE the restart rather than being reported
    alongside this end-of-deploy note. REPORTS by name, never AUTO-SEEDS (thread a25365a9's
    own ask, argued in the commit this lands with): `seed_default_compositions` upserts
    every default's spec unconditionally, including ones already present — running it
    automatically on every deploy would silently overwrite a default a human hand-edited
    live in the composer (the whole point of a composition being forkable/savable), trading
    today's dishonest-but-passive miscount for a silent, active clobber. A migration auto-
    applies safely because it replays a reviewed, versioned script; a composition auto-seed
    would replay code OVER whatever the DB now holds under that name. Reporting by name
    keeps the fix in the same class as the ratchet: name what's missing, let a human decide.

    ALSO CHECKS DRIFT, NOT ONLY ABSENCE (obligation e4612853, ruling 38c71544) — a composition
    can EXIST under the right name and still be silently stale: `composition_drift_notes`
    compares each DEFAULT_COMPOSITIONS entry's spec against the live DB row's own spec,
    exhaustively, name by name. Measured live before this shipped: 0/29 currently drifted —
    but that baseline is the CHEAPEST moment this check will ever have (any future nonzero
    reading is unambiguous new drift, not archaeology through a pre-existing backlog). Same
    ALARM-not-refuse posture as every other note this function returns: `cmd_deploy` prints
    these under "UN-RUN STEPS:" and never touches its own exit code over them — a drifted
    composition is a stale READ, not corruption, and refusing an unrelated deploy over it
    would repeat the exact false-refusal cost core.hooksPath's own finding (ruling d4e65da0)
    already proved real tonight.

    Also names every composition with room_id IS NULL (ruling 89e67c49) — a second, distinct
    gap class from a missing default, folded into the same end-of-deploy report rather than
    a separate command, since both are "a composition is silently unreachable" findings."""
    from src.orchestrator.compositions import DEFAULT_COMPOSITIONS

    rows = await pool.fetch("SELECT name, spec FROM compositions")
    have = {r["name"] for r in rows}
    live_specs = {r["name"]: (json.loads(r["spec"]) if isinstance(r["spec"], str) else r["spec"])
                 for r in rows}
    notes = composition_gap_notes(have, set(DEFAULT_COMPOSITIONS))
    notes += composition_drift_notes(live_specs, DEFAULT_COMPOSITIONS)
    unassigned = [r["name"] for r in
                 await pool.fetch("SELECT name FROM compositions WHERE room_id IS NULL")]
    notes += composition_room_gap_notes(unassigned)
    return notes


async def _run_casefold_automerge(pool: asyncpg.Pool) -> list[str]:
    """#108 PIECE 2 WIRING (obligation 5f7dfebb, operator ruling 22d47acb + the standing
    word "capitalization merging should be automatic not bottlenecked by me") — the ONE
    trigger site casefold_auto_merge_candidates' own docstring said nobody had built yet.
    A post-migration deploy step, not tied to the restart: this only ever touches the
    graph (SoftwareProject casefold twins), never code or a running service, so it runs
    once migrations are confirmed applied and well before anything restarts.

    ALWAYS surveys (dry-run report, every candidate and every skip named — never a
    silent drop, matching the underlying verb's own law). EXECUTES BY DEFAULT — the
    operator's own word: "automatic, not bottlenecked by me". `osiris deploy` is hand-
    invoked with no wrapper/cron to hang a "set it in the deploy env" on, so the flip is
    the default itself: OSIRIS_CASEFOLD_AUTOMERGE=0 opts a run OUT (any other value,
    including unset, executes). Either way every candidate goes through the SAME
    normalize_project_casing/merge() door with its own belief-gate — this function never
    re-derives that logic, only decides whether to pass execute."""
    from src.actions.core import Actions
    from src.orchestrator.projects import casefold_auto_merge_candidates

    execute = os.environ.get("OSIRIS_CASEFOLD_AUTOMERGE") != "0"
    result = await casefold_auto_merge_candidates(
        Actions(pool), evidence="osiris deploy: automatic casefold merge "
        "(#108 piece 2, operator ruling 22d47acb/d02f2cdd)",
        actor="osiris-deploy", execute=execute)
    notes = [f"casefold auto-merge: {'EXECUTED' if execute else 'dry-run'} — "
             f"{len(result['candidates'])} candidate(s), {len(result['skipped'])} skipped"]
    for c in result["candidates"]:
        notes.append(f"  {c['phantom']} -> {c['populated']} (correct case "
                     f"{c['correct_case']!r})")
    for s in result["skipped"]:
        notes.append(f"  SKIPPED: {s['canonicals']} — {s['reason']}")
    return notes


async def _run_pg_autotune_on_deploy(pool: asyncpg.Pool) -> str:
    """Ruling 45b251ed leg (a) - recomputes Postgres GUCs from THIS host's live RAM/CPU
    and the measured daemon envelope on every deploy, not just on the daily timer
    (deploy/osiris-pg-autotune.timer). Fail-open like every other deploy-time check
    beside it: a tuning failure degrades to a printed note, never blocks or fails the
    deploy. Never restarts postgres itself; see pg_autotune.py's own docstring."""
    try:
        from src.orchestrator.pg_autotune import apply_tuning, plan_tuning
        from src.orchestrator.pool_health import pg_activity_by_app

        health = await pg_activity_by_app(pool)
        fixed_budget = health.get("fixed_budget") or 56
        plan = await plan_tuning(pool, fixed_budget=fixed_budget)
        if not plan["changes"]:
            return "pg autotune: current GUCs already within range - nothing to apply"
        result = await apply_tuning(pool, plan)
        bits = [f"{c['name']} {c['before']}->{c['after']}" for c in result["applied"]]
        note = (f"pg autotune: applied {', '.join(bits)}" if bits
                else "pg autotune: nothing reloadable to apply")
        if result["deferred"]:
            deferred_bits = [f"{c['name']} {c['before']}->{c['after']}"
                              for c in result["deferred"]]
            note += (f" | pending change requiring a human-run restart, persisted not "
                     f"applied: {', '.join(deferred_bits)}")
        return note
    except Exception as exc:  # noqa: BLE001 - fail-open, never blocks a deploy
        return f"pg autotune: could not tune ({exc}) - deploy continues"


async def _run_remote_url_automerge(pool: asyncpg.Pool) -> list[str]:
    """#108 PIECE 3 WIRING, conservative first cut (scope: decision 2ee34a9d; build:
    Thoth's dispatch msg 4990/4973/4975) — a second post-migration deploy step beside
    casefold's, same shape: ALWAYS surveys (every candidate/skip named, never silent).
    EXECUTES under the SAME OSIRIS_CASEFOLD_AUTOMERGE default as piece 2 (0 opts out,
    anything else — including unset — executes) rather than a second env var: both are
    the same standing autonomy ruling (22d47acb) over the same class of act (a
    deterministic-signal SoftwareProject merge with its own belief-gate), so a second
    knob would only be a second thing to forget to set. Every candidate still goes
    through the SAME fold_project door with its own contradiction gate — this wiring
    never re-derives that logic, only decides whether to pass execute."""
    from src.actions.core import Actions
    from src.orchestrator.projects import remote_url_duplicate_candidates

    execute = os.environ.get("OSIRIS_CASEFOLD_AUTOMERGE") != "0"
    result = await remote_url_duplicate_candidates(
        Actions(pool), evidence="osiris deploy: automatic remote_url-matched merge "
        "(#108 piece 3, decision 2ee34a9d)",
        actor="osiris-deploy", execute=execute)
    notes = [f"remote_url auto-merge: {'EXECUTED' if execute else 'dry-run'} — "
             f"{len(result['candidates'])} candidate(s), {len(result['skipped'])} skipped"]
    for c in result["candidates"]:
        notes.append(f"  {c['dupe']} -> {c['into']} (remote_url {c['remote_url']!r})")
    for s in result["skipped"]:
        notes.append(f"  SKIPPED: {s['canonicals']} — {s['reason']}")
    return notes


MigrationState = Callable[[asyncpg.Pool, Path], Awaitable[tuple[str | None, str | None]]]
MigrateRunner = Callable[[Path], Awaitable[None]]


async def _real_migration_state(
    pool: asyncpg.Pool, repo_root: Path,
) -> tuple[str | None, str | None]:
    current = await pool.fetchval("SELECT version_num FROM alembic_version")
    return current, _alembic_head(repo_root)


async def _real_run_migrations(repo_root: Path) -> None:
    """The one place this module ever actually runs `alembic upgrade head` — IN-PROCESS via
    alembic's own command API (`tests/conftest.py`'s own pattern for the test DB), never a
    subprocess `alembic` rune: a bare `alembic` invocation connects to the prod-shaped 5432
    default because alembic/env.py's `os.environ.get("DATABASE_URL", ...)` sees whatever the
    CALLING shell happened to export (usually nothing on this dev box) — exactly the class
    ruling 45b074bf bans. Running it in-process means `apply_dev_fallback()` (already called
    by whichever command reached here — `cmd_migrate`/`cmd_deploy`) has ALREADY set
    os.environ["DATABASE_URL"] before this ever executes, so env.py reads the right value
    with no rune, no passthrough, nothing for a human to get wrong. `command.upgrade` is
    synchronous (real DDL, not worth a fake async wrapper) — off the event loop via
    to_thread, same discipline as every other blocking call this module makes."""
    from alembic import command

    cfg = _alembic_config(repo_root)
    if cfg is None:
        raise RuntimeError(f"no alembic.ini found under {repo_root}")
    await asyncio.to_thread(command.upgrade, cfg, "head")


async def _apply_pending_migrations(
    pool: asyncpg.Pool, repo_root: Path, *,
    state: MigrationState = _real_migration_state,
    run_migrations: MigrateRunner = _real_run_migrations,
) -> tuple[bool, str]:
    """THE MIGRATION GATE (thread c4681c38 leg 2): compares FIRST and refuses-or-runs BEFORE
    any restart, so a deploy is atomic from the schema's point of view. Batch 6's own near
    miss is exactly what this closes: the old order restarted services onto new code, THEN
    reported the pending migration as an end-of-deploy note — a window where new code ran
    against the old schema, surviving only because the new writes happened to be fail-open.
    Returns (ok, note); ok=False means REFUSE — the caller must not restart anything past
    this point. head=None (no alembic.ini under this repo_root — e.g. a test fixture's
    tmp_path) is undeterminable, not a mismatch, and never gates — same non-blocking
    discipline `alembic_gap_note` already establishes."""
    current, head = await state(pool, repo_root)
    gap = alembic_gap_note(current, head)
    if gap is None:
        return True, ("migrations: up to date" if head is not None else
                      "migrations: undeterminable here (no alembic.ini under this repo_root) "
                      "— not gating")
    # NAME THE ACCIDENTAL CONTROL (decision 8d3f5e2d, task #142 follow-up): this exact
    # refusal already happened once by luck — `command.upgrade(cfg, "head")` errors when
    # `current` isn't reachable from the tree's own alembic chain, and the generic except
    # below caught that and reported it as an opaque upgrade failure. Checking it here
    # FIRST makes the same refusal deliberate, with the real reason named, instead of
    # depending on alembic's own exception text to explain it. `known is False` is the ONLY
    # new branch — True or None (undeterminable, e.g. no alembic.ini here) fall through to
    # the unchanged path below, so every existing caller's behavior is preserved exactly.
    known = _alembic_revision_known(repo_root, current) if current is not None else None
    if known is False:
        return False, (
            f"migrations: REFUSED — DB is at revision {current!r}, which this tree's own "
            f"migrations do not recognize (decision 8d3f5e2d: another branch's migration "
            f"ran against this shared database before merging). NOTHING was restarted; "
            f"find and merge the branch that owns revision {current!r}.")
    try:
        await run_migrations(repo_root)
    except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, refuse, never restart
        return False, f"migrations: REFUSED — {gap} ({exc}). NOTHING was restarted."
    return True, f"migrations: {current!r}..{head!r} applied"


async def cmd_migrate(
    *, check: bool = False, repo_root: Path | None = None, pool: asyncpg.Pool | None = None,
    state: MigrationState = _real_migration_state,
    run_migrations: MigrateRunner = _real_run_migrations,
) -> int:
    """osiris migrate [--check] (thread c4681c38 leg 1): the ENV-CORRECT migration verb —
    `apply_dev_fallback()` runs before alembic ever reads DATABASE_URL (see
    `_real_run_migrations`'s own docstring for the exact footgun this closes: a bare
    `alembic upgrade head` silently targeting the prod-shaped 5432 default). `--check` only
    REPORTS a pending revision — never applies — for a human (or `osiris deploy`'s own gate,
    leg 2) who wants to know without acting."""
    root = repo_root if repo_root is not None else _find_repo_root()
    if root is None:
        print("osiris migrate: not inside a git repository — cd into the osiris checkout "
              "first.", file=sys.stderr)
        return 1

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=2,
                application_name="osiris-cli:migrate")
        except Exception as exc:  # noqa: BLE001
            print(f"osiris migrate: could not reach postgres at {settings.database_url} — "
                  f"{exc}.", file=sys.stderr)
            return 1
    try:
        current, head = await state(pool, root)
        if head is None:
            print(f"osiris migrate: no alembic.ini/alembic/ under {root} — nothing to "
                  "migrate here.", file=sys.stderr)
            return 1
        gap = alembic_gap_note(current, head)
        if gap is None:
            print(f"osiris migrate: up to date (revision {head!r})")
            return 0
        if check:
            print(f"osiris migrate --check: PENDING — {gap}")
            return 1
        try:
            await run_migrations(root)
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris migrate: upgrade failed — {exc}", file=sys.stderr)
            return 1
        print(f"osiris migrate: applied {current!r} -> {head!r}")
        return 0
    finally:
        if owns_pool:
            await pool.close()


async def _real_list_tools() -> dict[str, str] | str:
    from src.orchestrator.mcp_client import list_mcp_tools

    return await list_mcp_tools(await _mcp_url())


ListTools = Callable[[], Awaitable[dict[str, str] | str]]


async def _real_record_deploy(pool: asyncpg.Pool, repo_root: Path) -> str | None:
    """The write half of the reboot-is-a-deploy confession (thread 489a39d0): the boot-time
    guard (deploy_guard.check_unreviewed_boot) needs a ground truth to confess against, and
    none existed — this is it. A watermark (the same generic cursor store pulse.py's own
    `devhead:` already uses, not a new table): the ONLY place this repo's HEAD is meant to
    reach a running service is a successful `osiris deploy` restart, so recording it here IS
    the ledger. Returns the head it recorded (or None on a read failure, which is also a
    no-op — never a deploy failure, the write side stays as fail-open as the read side)."""
    from src.orchestrator.deploy_guard import _DEPLOY_CURSOR_KEY, _git_head
    from src.orchestrator.monitor import set_cursor

    head = _git_head(repo_root)
    if head is not None:
        await set_cursor(pool, _DEPLOY_CURSOR_KEY, head)
    return head


RecordDeploy = Callable[[asyncpg.Pool, Path], Awaitable[str | None]]
WaitForHealth = Callable[[], Awaitable[tuple[bool, float]]]
WaitForSmoke = Callable[[], Awaitable[tuple[list[str], float]]]
CheckWhisperProbe = Callable[[], Awaitable[tuple[bool, str]]]
ChaosGate = Callable[[asyncpg.Pool], Awaitable[dict[str, Any]]]
FullSuiteGate = Callable[[Path], Awaitable[dict[str, Any]]]
CheckFalseMintLive = Callable[[asyncpg.Pool], Awaitable[list[dict[str, Any]]]]


async def _synthetic_automount_probe(client: Any) -> tuple[bool, str]:
    """The pure verdict logic, `client`-injected (same pattern `smoke_chrome` uses) so it's
    testable against an `httpx.MockTransport` with no live server — `_real_check_whisper_
    probe` is the thin real-client wrapper cmd_deploy actually calls. POSTs a THROWAWAY
    /automount call, then immediately /session-end to release whatever row it minted —
    this probe leaves nothing behind. Non-200, a network failure, OR a 200 whose own body
    carries `{"error": ...}` (the exact silent-failure shape 33a3573 fixed once already,
    task #179's own headline) all refuse — the route degrading gracefully to a 200-with-
    error would defeat the entire point of this gate."""
    import uuid as _uuid

    sid = f"deploy-probe-{_uuid.uuid4().hex[:12]}"
    try:
        r = await client.post("/automount", json={"session_id": sid, "cwd": "/tmp"})
        try:
            await client.post("/session-end", json={"session_id": sid})
        except Exception:  # noqa: BLE001 — best-effort cleanup, never the gate's own verdict
            pass
        if r.status_code != 200:
            return False, (f"whisper probe: REFUSED — /automount returned "
                          f"{r.status_code}: {r.text[:200]}")
        body = r.json()
        if isinstance(body, dict) and body.get("error"):
            return False, (f"whisper probe: REFUSED — /automount returned 200 with an "
                          f"error body: {body['error']}")
        return True, "whisper probe: /automount round-tripped clean"
    except Exception as exc:  # noqa: BLE001
        return False, f"whisper probe: REFUSED — /automount round-trip failed: {exc}"


async def _real_check_false_mint_live(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """DEPLOY GATE (operator ruling 921eabcf, addendum to obligation 6b1efacb, 2026-08-18:
    "prevent weird forking like that and reject it architecturally"): a generation
    carrying false_mint=true with a LIVE mount is a candidate for the exact zero-turn
    phantom fold blindness the halcyon incident named — this must read ZERO harness-
    confirmed specimens before a deploy is recorded. Same base query graph_lint's own
    `false-mint-live` check runs (compositions.py's `_fn_lint`), duplicated here as a
    plain, fast, single-purpose query rather than routing a deploy gate through the full
    lint composition machinery for one check.

    ONE LIVENESS AUTHORITY, FOURTH DOOR (Thoth msg 5719, 2026-08-26, thread 2c3c2b9a): a
    fresh/refreshing `agent_mounts` row is NOT proof of a live body — the SAME "cache in
    both directions" law `is_occupied_by_a_live_body` exists to enforce everywhere else
    (register_agent/mount, FleetView claim, launch_seat, mailbox's send-to-lineage check,
    phantom_fold_reap's own reinstate bucket). This door used to trust the mount row
    alone; a real incident (agent:0123dec2-ii, project atlas) proved that wrong — the
    flagged id's own mount row was fresh, but registry_census showed NO body under it;
    the real live body sat under a DIFFERENT generation id entirely. Each candidate is now
    cross-checked against that SAME authority: `harness_confirmed_live=True` is the actual
    halcyon shape (a genuinely live body wrongly folded — `reinstate_generation` is the
    correct repair); `harness_confirmed_live=False` is a DIFFERENT anomaly this door must
    still refuse on, but must NEVER recommend `reinstate_generation` for — doing so would
    resurrect a bodiless generation, manufacturing the exact phantom a correct fold
    already cleaned up (the inverse of #190's Deckard case). Returns one dict per
    offending canonical (empty = clean); `cmd_deploy` owns picking the remedy text per
    bucket, never this function (a query has no business writing prose)."""
    from src.orchestrator.agents import is_occupied_by_a_live_body

    rows = await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Agent' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='false_mint' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  = 'true' "
        "AND EXISTS (SELECT 1 FROM agent_mounts m WHERE m.agent_id=o.canonical "
        "  AND m.last_seen > now() - interval '900 seconds') "
        "ORDER BY o.canonical")
    out: list[dict[str, Any]] = []
    for r in rows:
        occupied = await is_occupied_by_a_live_body(pool, r["canonical"])
        out.append({"agent_id": r["canonical"], "harness_confirmed_live": occupied})
    return out


async def _real_check_whisper_probe() -> tuple[bool, str]:
    """POST a THROWAWAY /automount call against the just-restarted server (task #179) —
    the same law as the migration gate: a deploy that cannot prove the whisper's own
    server half actually works must not be recorded as a success, only reported."""
    import httpx

    from src.config.settings import get_settings

    settings = get_settings()
    base = f"http://{settings.osiris_mcp_host}:{settings.osiris_mcp_port}"
    async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
        return await _synthetic_automount_probe(client)


async def cmd_deploy(
    *, repo_root: Path | None = None, git_status: GitStatus = _real_git_status,
    restart: RestartServices = _real_restart_services, pool: asyncpg.Pool | None = None,
    list_tools: ListTools = _real_list_tools,
    migration_state: MigrationState = _real_migration_state,
    run_migrations: MigrateRunner = _real_run_migrations,
    record_deploy: RecordDeploy = _real_record_deploy,
    wait_for_health: WaitForHealth = _wait_for_health,
    wait_for_smoke: WaitForSmoke = _wait_for_smoke,
    install_units: InstallUserUnits = _real_install_user_units,
    check_whisper_probe: CheckWhisperProbe = _real_check_whisper_probe,
    chaos_gate: ChaosGate = _real_chaos_gate,
    check_false_mint_live: CheckFalseMintLive = _real_check_false_mint_live,
    full_suite_gate: FullSuiteGate = _real_full_suite_gate,
    deploy_settings: Settings | None = None,
) -> int:
    """The deploy ritual as one verb (thread e51a841c): a live near-miss held batch 3 because
    src/orchestrator/handshake.py carried another agent's uncommitted WIP and the three
    services import straight from the working tree — only a by-hand `git status` caught it
    before a restart would have shipped a half-written identity edit. Replaces that by-hand
    protocol: (1) refuse on a dirty tracked src/ tree, naming the files (never guesses whose
    WIP it is — check project mail for a collision-watch broadcast instead of trusting a
    fragile heuristic); (2) compare migrations and refuse-or-run them BEFORE anything
    restarts (thread c4681c38 leg 2 — batch 6's own near miss: the old order restarted onto
    new code, then only reported the pending migration AFTER, a window where new code ran
    against the old schema); (3) restart osiris-mcp/worker/console; (4) run smoke,
    per-surface, with a bounded wait-for-up so a still-binding uvicorn never reads as a false
    failure; (5) name any un-run seeder step by comparison, never by assumption. Just before
    the restart, installs every deploy/user/*.service file over ~/.config/systemd/user/ and
    daemon-reloads if anything changed (thread e6fd3772 piece 3-infra) — this box's dev-unit
    config rides the deploy instead of a hand-authored divergence from deploy/. REFUSES (before
    restarting) if deploy/user/ carries files but install_units reports NOTHING — a live
    specimen restarted onto a stale unit set while the deploy log showed zero `unit:` lines
    and still exited 0; loud failure beats a quiet one restarting onto whatever was already
    there. Also names
    (informationally, never gating) any dirty COMMIT-DEPLOYED script — a oneshot timer unit
    reads straight off disk, so nothing here can hold it back (msg 1481) — and (thread
    6a78e64b leg 2) diffs the MCP tool list before vs after the restart, so a deploy names
    exactly which verbs are arriving rather than leaving that to be discovered by accident.
    (6) POSTs a THROWAWAY /automount to the just-restarted server (task #179) and REFUSES to
    record the deploy on anything but a clean round-trip — same law as the migration gate:
    the ledger must never claim a deploy the whisper's own server half cannot actually serve.
    (7) records the deployed HEAD (thread 489a39d0) — the ground truth the reboot-is-a-deploy
    boot guard confesses against; a raw restart or a reboot never calls this, so the ledger
    and reality staying in sync is itself evidence the deploy went through this ritual.

    `wait_for_health`/`wait_for_smoke` default to the REAL bounded pollers (120s/30s
    ceilings, real network round-trips against the live console/MCP) — injectable for the
    same reason every other side-effecting dependency here is: a test exercising cmd_deploy's
    own control flow (order of operations, what it prints, what it returns) has no reason to
    also pay for a live round-trip against production services it isn't testing (task #165,
    2026-08-09 — seven tests were doing exactly that, unmocked, 565s of a 1160s suite)."""
    root = repo_root if repo_root is not None else _find_repo_root()
    if root is None:
        print("osiris deploy: not inside a git repository — cd into the osiris checkout "
              "first.", file=sys.stderr)
        return 1

    status = git_status(root)
    dirty_src = dirty_tracked_src_files(status)
    if dirty_src:
        print("osiris deploy: REFUSED — tracked src/ files have uncommitted changes:")
        for f in dirty_src:
            print(f"  - {f}")
        print("Restarting now would ship a half-written edit. Commit or stash first — check "
              "project mail for a collision-watch broadcast naming these files before "
              "assuming they're abandoned.")
        return 1

    for note in commit_deployed_notes(status, oneshot_deployed_scripts(root)):
        print(f"NOTE: {note}")

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=2,
                application_name="osiris-cli:deploy")
        except Exception as exc:  # noqa: BLE001
            print(f"osiris deploy: REFUSED — could not reach postgres to check migrations "
                  f"— {exc}. NOTHING was restarted.", file=sys.stderr)
            return 1
    try:
        from src.orchestrator.deploy_guard import (
            _DEPLOY_CURSOR_KEY,
            _git_head,
            alarm_withheld_deploy_record,
            check_diverged_since_last_deploy,
        )
        from src.orchestrator.monitor import get_cursor

        # Captured BEFORE `record_deploy` below overwrites this same cursor to the NEW
        # HEAD (obligation 8752024d) — merge_claim_hygiene needs the ref THIS deploy is
        # walking FROM, not the one it's about to record itself as having reached.
        previously_deployed = await get_cursor(pool, _DEPLOY_CURSOR_KEY)

        diverged = await check_diverged_since_last_deploy(pool, repo_root=root)
        if diverged:
            print(f"WARNING: {diverged}")

        migrated_ok, migration_note = await _apply_pending_migrations(
            pool, root, state=migration_state, run_migrations=run_migrations)
        print(migration_note)
        if not migrated_ok:
            return 1

        for note in await _run_casefold_automerge(pool):
            print(note)
        for note in await _run_remote_url_automerge(pool):
            print(note)

        expects_unit_install = bool(user_unit_sources(root))
        unit_notes = await install_units(root)
        for note in unit_notes:
            print(note)
        if expects_unit_install and not unit_notes:
            print("osiris deploy: REFUSED — deploy/user/ carries unit files but install_units "
                  "reported NOTHING (a silent no-op, thread e6fd3772 piece 3-infra's own "
                  "specimen: a real deploy restarted onto a stale unit set while printing zero "
                  "unit: lines). Refusing rather than restarting blind. NOTHING was restarted.",
                  file=sys.stderr)
            return 1

        tools_before = await list_tools()

        rc, out = await restart(list(DEPLOY_UNITS))
        if rc != 0:
            print(f"osiris deploy: restart failed (exit {rc}): {out}", file=sys.stderr)
            return 1
        print(f"osiris deploy: restarted {', '.join(DEPLOY_UNITS)}")

        health_ready, health_waited = await wait_for_health()
        if health_ready:
            print(f"health: up after {health_waited:.0f}s" if health_waited
                  else "health: up immediately")
        else:
            print(f"health: NOT UP after waiting {health_waited:.0f}s (ceiling) — the "
                  "console did not come up; this is a real startup failure, not a "
                  "smoke-timing false-alarm")

        whisper_ok, whisper_note = await check_whisper_probe()
        print(whisper_note)
        if not whisper_ok:
            print("osiris deploy: NOT recording this deploy — the whisper's own server "
                  "half cannot be trusted after a restart it cannot itself verify.")
            return 1

        # THE HALCYON GATE (operator ruling 921eabcf, addendum to obligation 6b1efacb,
        # 2026-08-18): "prevent weird forking like that and reject it architecturally" —
        # a false_mint generation with a LIVE mount must read ZERO before a deploy is
        # recorded, always on (no kill switch — this is a cheap read, not a SIGKILL).
        false_mint_live = await check_false_mint_live(pool)
        if false_mint_live:
            confirmed = [r["agent_id"] for r in false_mint_live if r["harness_confirmed_live"]]
            unconfirmed = [r["agent_id"] for r in false_mint_live
                           if not r["harness_confirmed_live"]]
            print("osiris deploy: REFUSED — false-mint-live: a generation carries "
                  "false_mint=true with a live mount.")
            reason_lines = []
            if confirmed:
                line = ("HARNESS-CONFIRMED LIVE (the halcyon shape — a genuinely live body "
                        f"wrongly folded): {', '.join(confirmed)}. reinstate_generation is the "
                        "repair door.")
                print(f"  {line}")
                reason_lines.append(line)
            if unconfirmed:
                line = ("NOT harness-confirmed live (a fresh/refreshing mount row alone is "
                        "not proof of a live body): "
                        f"{', '.join(unconfirmed)}. Do NOT run reinstate_generation on these — "
                        "that would resurrect a bodiless generation. The real live body may "
                        "sit under a DIFFERENT generation id; a human must reconcile identity.")
                print(f"  {line}")
                reason_lines.append(line)
            print("osiris deploy: NOT recording this deploy.")
            # THE WITHHELD-RECORD CONFESSION (thread 3b34f6c5, #52's own law): the refusal
            # above is correct, but recording nothing leaves the ledger silently stale — a
            # mechanism producing "unrecorded completion" on purpose. The code IS deployed
            # and healthy at this point (restart/health/whisper already passed); only the
            # ledger write was withheld.
            running_head = _git_head(root)
            if running_head is not None:
                with contextlib.suppress(Exception):  # a confession must never crash the CLI
                    await alarm_withheld_deploy_record(
                        pool, running_head=running_head,
                        reason="false-mint-live: " + " ".join(reason_lines))
            return 1

        # THE FULL SUITE ON THE MERGED TREE (task #186, Thoth DM 5637, 2026-08-25) — OFF
        # by default, same law as the chaos gate below it. Runs BEFORE the chaos gate: a
        # suite that doesn't even pass makes a slow SIGKILL replay pointless. Neither of
        # tonight's two live incidents (a branch's own scoped-gate green, false once
        # merged; an auto-merged capture.py only proven correct by a full re-run) was a
        # daemon-crash-resilience gap — this is the gate that actually covers them.
        #
        # `deploy_settings` IS INJECTABLE, DELIBERATELY (thread be24817b, the self-refuting
        # gate): arming this flag via env makes `full_suite_gate` spawn pytest as a
        # subprocess that INHERITS that same env — including onto
        # tests/test_cli.py::test_cmd_deploy_skips_the_full_suite_gate_by_default, which
        # calls THIS function again to assert the flag is off "by default." Reading ambient
        # `get_settings()` there was testing the environment the gate itself had just set,
        # never the actual default in source — arming was guaranteed to refuse itself. A
        # "skips by default" test now passes an EXPLICITLY CONSTRUCTED `Settings` object
        # instead, so its claim is about the field's real default, not about whatever
        # happens to be armed in the process that is running it.
        from src.config.settings import get_settings as _get_deploy_settings

        active_settings = deploy_settings if deploy_settings is not None \
            else _get_deploy_settings()
        if active_settings.osiris_deploy_full_suite_gate:
            suite_report = await full_suite_gate(root)
            if suite_report["ok"]:
                print("full suite: green on the merged tree")
            else:
                print("osiris deploy: REFUSED — the full suite failed on the merged tree:")
                print(suite_report["summary"])
                print("NOT recording this deploy.")
                return 1

        # CRASH REPLAY AS A GATE (Thoth msg 5338, 2026-08-18) — OFF by default, the same
        # law as osiris_trigger_enabled/osiris_pit_watch_enabled: a mechanism that SIGKILLs
        # a live service earns its own kill switch, never inherits one. When on, runs a
        # SECOND, harsher restart cycle (kill -9 + a concurrent session-end storm, not the
        # graceful `restart` above) and refuses the deploy outright on any finding — this
        # is a GATE (577988ed's fail-open clause is for infrastructure this can't control,
        # never for a genuine invariant violation this module exists to catch).
        if active_settings.osiris_deploy_chaos_gate:
            import json

            from src.orchestrator.monitor import set_cursor

            chaos_report = await chaos_gate(pool)
            await set_cursor(pool, "chaos-replay:last", json.dumps(chaos_report))
            if chaos_report["ok"]:
                print(f"chaos replay: all invariants held — {chaos_report['storm_fired']} "
                      f"session-end(s) fired concurrently with the kill, recovered in "
                      f"{chaos_report['recovery_elapsed_secs']:.0f}s")
            else:
                print("osiris deploy: REFUSED — the chaos replay gate found a real "
                      "invariant violation:")
                for f in chaos_report["findings"]:
                    print("  -", f)
                print("NOT recording this deploy.")
                return 1

        deployed_head = await record_deploy(pool, root)
        print(f"deploy ledger: recorded {deployed_head}" if deployed_head else
              "deploy ledger: HEAD unknown — not recorded (repo_root isn't a git checkout)")

        # #189 ADOPTION METER (Thoth msg 5825, ruling d68c57e5) — an INSTRUMENT, never a
        # gate: read-only against the graph (its one write is a baseline watermark, seeded
        # at most once), printed on every deploy so nobody has to remember to run
        # triage(mode='census') by hand and compare against a number quoted in a decision's
        # prose — exactly the shape that let #189's own diagnosis (5169686b) sit unmeasured
        # for 24 days while the population it named grew ~1,800.
        from src.orchestrator.adoption_meter import adoption_meter, render_adoption_line

        meter = await adoption_meter(pool)
        print(render_adoption_line(meter))

        fails, waited = await wait_for_smoke()
        if fails:
            print(f"SMOKE FAILURES (after waiting {waited:.0f}s for the restart to come up):")
            for f in fails:
                print(" -", f)
        elif waited:
            print(f"smoke: all green (came up after {waited:.0f}s)")
        else:
            print("smoke: all green")

        tools_after = await list_tools()
        if isinstance(tools_before, str) or isinstance(tools_after, str):
            side = "before" if isinstance(tools_before, str) else "after"
            print(f"tool list: could not compare — the {side}-restart round-trip failed "
                  f"({tools_before if side == 'before' else tools_after})")
        else:
            delta = diff_tool_lists(tools_before, tools_after)
            if delta:
                print(f"TOOL LIST CHANGED: {', '.join(delta)} — connected sessions see the "
                      "old list until their own client refreshes.")
            else:
                print("tool list: unchanged")

        gaps = await _composition_gaps(pool)
        if gaps:
            print("UN-RUN STEPS:")
            for g in gaps:
                print(" -", g)
        else:
            print("compositions: up to date")

        from src.orchestrator.deploy_guard import (
            landing_audit,
            local_ref_hygiene,
            merge_claim_hygiene,
            origin_visibility,
            venv_import_hygiene,
        )
        print(await origin_visibility(root))
        print(await local_ref_hygiene(root))
        print(await merge_claim_hygiene(root, since=previously_deployed))
        print(await venv_import_hygiene(root))
        print(await _run_pg_autotune_on_deploy(pool))

        from src.actions.core import Actions as _Actions

        audit = await landing_audit(_Actions(pool), root)
        if audit["stale_unmerged_branches"] or audit["graph_claim_mismatches"]:
            print(f"landing audit: {len(audit['stale_unmerged_branches'])} stale branch(es), "
                  f"{len(audit['graph_claim_mismatches'])} graph claim mismatch(es) — "
                  f"{len(audit['obligations'])} obligation(s) minted/deduped")
        else:
            print("landing audit: clean — every branch is either merged or held-work-claimed, "
                  "no graph text disagrees with git")

        from scripts.push_guard import hook_status
        print(hook_status(root))

        from scripts.gate_hook import hook_status as gate_hook_status
        print(gate_hook_status(root))

        return 1 if fails else 0
    finally:
        if owns_pool:
            await pool.close()


# --- merge / unmerge ---------------------------------------------------------------------------

async def cmd_merge(
    dupe: str, into: str, evidence: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris merge <dupe> <into> --evidence <text> [--actor <who>] — the console-script
    door onto orchestrator.merge.merge, the SAME function the merge MCP tool wraps (no
    duplicated logic, no softened gate). SELF-TYPING, exactly like the MCP tool: `dupe`'s
    own form picks Agent/Seat/SoftwareProject (agent:.../seat:.../else) — this is NOT
    fold-project's old SoftwareProject-only behavior wearing a new name, it is the full
    merge surface, dispatch 3683's own finding that the two doors had drifted apart.

    THE SANCTIONED SECOND DOOR (thread 2446, formerly fold-project's): the MCP tool can
    sit invisible in a live client's stale deferred-tool index across a deploy, or be
    unreachable to a worker whose sandbox classifier refuses a raw DATABASE_URL script —
    an installed entrypoint is a path that isn't the MCP index at all.

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT (thread 2474): the
    merge-event/same_as witness the MCP wrapper queries after the fact — SoftwareProject
    merges only, matching the MCP tool's own conditional exactly — is queried here too."""
    from src.actions.core import Actions
    from src.orchestrator.merge import _merge_type
    from src.orchestrator.merge import merge as _merge

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:merge")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris merge: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        out = await _merge(Actions(pool), dupe=dupe, into=into, evidence=evidence, actor=actor)
        if "error" not in out and _merge_type(dupe.strip()) == "SoftwareProject":
            witness = await pool.fetchrow(
                "SELECT oe.id AS merge_event_id, l.id AS same_as_link_id "
                "FROM objects d JOIN objects i ON i.canonical=$2 "
                "JOIN object_events oe ON oe.event_type='merge' AND oe.related_id=d.id "
                "  AND oe.object_id=i.id "
                "LEFT JOIN links l ON l.type='same_as' AND l.from_id=d.id AND l.to_id=i.id "
                "WHERE d.canonical=$1 ORDER BY oe.created_at DESC LIMIT 1",
                out["folded"], out["into"])
            if witness:
                out["merge_event_id"] = witness["merge_event_id"]
                out["same_as_link_id"] = witness["same_as_link_id"]
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris merge: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"folded {out['folded']} into {out['into']}")
    if out.get("edges_moved"):
        print("edges moved: " + ", ".join(f"{k}={v}" for k, v in out["edges_moved"].items()))
    if out.get("mounts_moved"):
        print(f"mounts moved: {out['mounts_moved']}")
    if out.get("merge_event_id") is not None:
        print(f"merge event: {out['merge_event_id']}  same_as link: "
              f"{out.get('same_as_link_id')}")
    return 0


async def cmd_fold_project(
    dupe: str, into: str, evidence: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """DEPRECATED ALIAS (dispatch 3683): fold_project no longer exists as an MCP tool —
    it collapsed into merge() (ruling 31c02dca, decision a926a8d0) and the CLI never
    followed, the exact "two halves of this house use different words for one act"
    specimen the operator's own consistency ask named. Kept working, hidden from the
    front-door listing, forwarding straight to cmd_merge with the identical arguments —
    never break a human's muscle memory silently, but never advertise the old name either."""
    print("osiris fold-project is deprecated — use `osiris merge` (identical arguments, "
          "same evidence-gated fold). Continuing as merge.", file=sys.stderr)
    return await cmd_merge(dupe, into, evidence, actor=actor, pool=pool)


async def cmd_unmerge(
    dupe: str, because: str, *, actor: str, execute: bool = False,
    pool: asyncpg.Pool | None = None, as_json: bool = False,
) -> int:
    """osiris unmerge <dupe> --because <text> [--actor <who>] [--execute] — the console-
    script door onto orchestrator.merge.unmerge, the SAME function the unmerge MCP tool
    wraps. DRY RUN IS THE DEFAULT, matching the MCP tool's own convention exactly: without
    --execute this returns the reversal PLAN (what would move back) and writes nothing;
    review it, then re-run with --execute. Self-typing off `dupe`'s own form, same rule
    as merge/cmd_merge. Built alongside merge's own CLI rename (dispatch 3683) — the two
    verbs are a pair on the MCP side and had no reason to stay asymmetric on this one."""
    from src.actions.core import Actions
    from src.orchestrator.merge import unmerge as _unmerge

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:unmerge")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris unmerge: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        out = await _unmerge(Actions(pool), dupe=dupe, because=because, actor=actor,
                             execute=execute)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris unmerge: refused — {out['error']}", file=sys.stderr)
        return 1
    from src import cli_render as render
    render.emit(out, as_json=as_json, title="unmerge")
    return 0


async def cmd_retention(
    table: str, *, days: int | None, execute: bool, batch_size: int = 5000,
    pool: asyncpg.Pool | None = None, as_json: bool = False,
) -> int:
    """osiris retention outbox|audit-log [--days N] [--execute] [--batch-size N] — thread
    e6fd3772 piece 1. COLD BY DEFAULT: without --execute this only COUNTS what's eligible
    and writes nothing; --execute deletes in batches (default 5000 rows/statement),
    looping until a batch comes back short. `table` selects which retention function
    (src.orchestrator.retention) runs; each has its own default window (outbox 30 days,
    audit_log 90) used when --days is omitted."""
    from src.orchestrator.retention import audit_log_retention, outbox_retention

    fn = {"outbox": outbox_retention, "audit-log": audit_log_retention}.get(table)
    if fn is None:
        print(f"osiris retention: unknown table {table!r} — outbox or audit-log",
              file=sys.stderr)
        return 1
    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:retention")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris retention: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        kwargs: dict[str, Any] = {"execute": execute, "batch_size": batch_size}
        if days is not None:
            kwargs["days"] = days
        out = await fn(pool, **kwargs)
    finally:
        if owns_pool:
            await pool.close()
    from src import cli_render as render
    render.emit(out, as_json=as_json, title="retention")
    if not execute:
        print(f"osiris retention: dry run — {out['eligible']} row(s) eligible, "
              "nothing deleted. Pass --execute to delete.", file=sys.stderr)
    return 0


# --- charter-for -------------------------------------------------------------------------------

async def cmd_charter_for(
    seat_id: str, repos: list[str], because: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris charter-for <seat> --repos a,b,c --because <text> --actor <who> — the
    console-script door onto charter.charter_for, the SAME function the charter_for MCP
    tool wraps (no duplicated guard, no softening: the managed_by/operator-actor check is
    the whole point of this verb and is exactly charter_for's own, untouched here — the
    one guard tonight that is genuinely ENFORCED rather than merely documented).

    THE SANCTIONED SECOND DOOR (thread 2474, the third occurrence of the same shape as
    fold_project/annotate_thread/amend_decision: a verb ships, deploys, and the fleet's
    live MCP clients cannot see it in their own deferred-tool index — not this module's
    bug, upstream per ruling 482c3d0f). An installed entrypoint bypasses that index
    entirely, the same class of thing as `osiris deploy`/`osiris fold-project`.

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT (the general rule thread
    2474 names after fold-project's CLI receipt was found silently weaker than its MCP
    twin): charter_for's own return dict IS the full receipt already — this command
    prints it whole, nothing dropped, so there is no second copy of the enrichment logic
    to drift out of sync."""
    from src.actions.core import Actions
    from src.orchestrator.charter import charter_for

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:charter-for")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris charter-for: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        out = await charter_for(Actions(pool), seat_id, repos, because=because, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris charter-for: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"charter for {out['seat']}: {out['charter']}")
    if out.get("added"):
        print("added: " + ", ".join(out["added"]))
    if out.get("removed"):
        print("removed: " + ", ".join(out["removed"]))
    if out.get("rejected"):
        print(f"rejected: {out['rejected']}")
    print(f"because: {out['because']}  declared by: {out['declared_by']}")
    return 0


# --- amend-practice ----------------------------------------------------------------------------

async def cmd_amend_practice(
    ref: str, amendment: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris amend-practice <ref> <amendment> --actor <who> — the console-script door onto
    capture.amend_practice, the SAME function the amend_practice MCP tool wraps (no
    duplicated guard: the refuted-practice refusal and the blank-amendment check are
    exactly amend_practice's own, untouched here).

    THE SANCTIONED SECOND DOOR (thread 06c3529b, the fourth occurrence of the same shape as
    fold_project/charter_for/annotate_thread/amend_decision: a verb ships, deploys, and the
    fleet's live MCP clients cannot see it in their own deferred-tool index — not this
    module's bug, upstream per ruling 482c3d0f rather than worked around).

    CALLS THE ORCHESTRATOR FUNCTION DIRECTLY, not cmd_fleet's call_mcp_tool round-trip
    (Thoth DM 3126/3127's own open question — recorded here per his instruction): an
    amendment is a WRITE, and a call_mcp_tool session is anonymous (no ctx, no mounted
    identity), so the MCP wrapper's own `_actor_for`/`_source_for` fallback would stamp it
    with the generic "session" bucket — a real provenance loss for a governance-relevant
    write. fold-project/charter-for already established the right precedent for exactly
    this class of write-through-CLI-door: own pool, explicit --actor, real attribution.
    (Consequence, named honestly: unlike cmd_fleet, this door does NOT prove the frozen
    tool-index is reachable server-side over the wire — it proves the underlying function
    works, a narrower claim. Thoth's own three-verbs-in-one-call test already carries the
    server-vs-client staleness proof; this door's job is unblocking the fleet, not
    re-proving that diagnosis.)

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT (thread 2474's general rule):
    mirrors the MCP wrapper's own {"id", "amendment", "status"} / {"error": ...} shape by
    hand, since capture.amend_practice itself returns a bare UUID | None and raises
    ValueError rather than shaping either receipt itself — the MCP tool's own try/except
    and None-check are duplicated here on purpose, not softened."""
    from src.actions.core import Actions
    from src.orchestrator.capture import amend_practice

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:amend-practice")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris amend-practice: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        try:
            pid = await amend_practice(Actions(pool), ref, amendment, source=actor)
        except ValueError as e:
            print(f"osiris amend-practice: refused — {e}", file=sys.stderr)
            return 1
    finally:
        if owns_pool:
            await pool.close()
    if pid is None:
        print(f"osiris amend-practice: refused — no practice matches {ref!r}",
              file=sys.stderr)
        return 1
    print(f"amended {pid}: {amendment.strip()}")
    return 0


# --- annotate-thread ---------------------------------------------------------------------------

async def cmd_annotate_thread(
    ref: str, note: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris annotate-thread <ref> <note> --actor <who> — the console-script door onto
    capture.annotate_thread, the SAME function the annotate_thread MCP tool wraps (no
    duplicated guard: the blank-note and no-match refusals are exactly annotate_thread's
    own, untouched here).

    NAMED BEFORE IT WAS BUILT: charter_for's own docstring already listed this verb
    (thread 2474) as sharing fold_project's shape — a verb ships, deploys, and the fleet's
    live MCP clients cannot see it in their own deferred-tool index (not this module's bug,
    upstream per ruling 482c3d0f) — but only fold_project/charter_for/amend_practice ever
    got the second door built. This closes that gap.

    CALLS THE ORCHESTRATOR FUNCTION DIRECTLY, amend_practice's own precedent: an annotation
    is a WRITE, and a call_mcp_tool round-trip is anonymous — the MCP wrapper's own
    `_actor_for` fallback would stamp it with the generic "session" bucket instead of a
    named actor, a real provenance loss for a governance-relevant write.

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT (thread 2474's general rule):
    mirrors the MCP wrapper's own {"id", "note", "status"} / {"error": ...} shape by hand."""
    from src.actions.core import Actions
    from src.orchestrator.capture import annotate_thread

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:annotate-thread")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris annotate-thread: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        try:
            tid = await annotate_thread(Actions(pool), ref, note, source=actor)
        except ValueError as e:
            print(f"osiris annotate-thread: refused — {e}", file=sys.stderr)
            return 1
    finally:
        if owns_pool:
            await pool.close()
    if tid is None:
        print(f"osiris annotate-thread: refused — no thread matches {ref!r}", file=sys.stderr)
        return 1
    print(f"annotated {tid}: {note.strip()}")
    return 0


async def cmd_rematerialize(
    anchor_sid: str, *, dest: str | None = None, force: bool = False,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris rematerialize <anchor_sid> [--dest PATH] [--force] — the console-script
    door onto SoulStore.rematerialize_to_disk, the SAME function the rematerialize MCP
    tool wraps (no duplicated guard: the live-transcript refusal and the broken-chain
    report are exactly rematerialize_to_disk's own, untouched here).

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT (thread 2474's general
    rule, same as annotate-thread above): mirrors the MCP wrapper's own dict shape by
    hand rather than a round-trip through the tool itself."""
    from src.ingest.soul_store import SoulStore

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:rematerialize")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris rematerialize: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        receipt = await SoulStore(pool).rematerialize_to_disk(
            anchor_sid, dest=dest, force=force)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in receipt:
        print(f"osiris rematerialize: refused — {receipt['error']}", file=sys.stderr)
        return 1
    print(f"wrote {receipt['written']} ({receipt['lines']} lines, "
          f"sha256 {receipt['sha256']})")
    return 0


# --- amend-decision ----------------------------------------------------------------------------

async def cmd_amend_decision(
    ref: str, addendum: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris amend-decision <ref> <addendum> --actor <who> — the console-script door onto
    capture.amend_decision, the SAME function the amend_decision MCP tool wraps (no
    duplicated guard: the blank-addendum and already-superseded refusals are exactly
    amend_decision's own, untouched here).

    NAMED BEFORE IT WAS BUILT (thread 2474, same gap annotate_thread's own CLI door
    above closes): shipped, deployed, invisible to a stale client's deferred-tool index
    (ruling 482c3d0f), but never given a second door until now.

    CALLS THE ORCHESTRATOR FUNCTION DIRECTLY (amend_practice's own precedent, same
    reason): a call_mcp_tool round-trip has no mounted identity to stamp the addendum
    with, only the generic "session" bucket — a real provenance loss.

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT: mirrors the MCP wrapper's
    own {"id", "addendum", "status"} / {"error": ...} shape by hand."""
    from src.actions.core import Actions
    from src.orchestrator.capture import amend_decision

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:amend-decision")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris amend-decision: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        try:
            did = await amend_decision(Actions(pool), ref, addendum, source=actor)
        except ValueError as e:
            print(f"osiris amend-decision: refused — {e}", file=sys.stderr)
            return 1
    finally:
        if owns_pool:
            await pool.close()
    if did is None:
        print(f"osiris amend-decision: refused — no decision matches {ref!r}", file=sys.stderr)
        return 1
    print(f"amended {did}: {addendum.strip()}")
    return 0


# --- mint-seat -----------------------------------------------------------------------------------

def _context_house(house: str | None) -> str | None:
    """The house to hunt for a lone manager candidate in: `--house` if given, else the
    cwd's own `.osiris` pin (`project = "..."`), else the cwd's own basename — the same
    fallback chain seats.resolve_project runs for an unseated caller (a raw terminal has
    no seated agent_id to short-circuit through, so the seated branch never applies here)."""
    if house:
        return house
    from src.orchestrator.agents import read_project_label

    pinned = read_project_label(os.getcwd())
    if pinned:
        return pinned
    return Path.cwd().name or None


async def _infer_manager(
    pool: asyncpg.Pool, house: str | None,
) -> tuple[str | None, str | None]:
    """(manager_handle, error). The SOLE existing seat in `house`, never a guess among
    several and never a fabricated manager for an empty house — crossing into a brand-new
    house always needs an explicit --manager naming a seat that already exists somewhere
    else (mint_seat's own cross-house guard requires it; an empty house has nothing to
    infer from by construction, and inventing one here would be exactly the silent-guess
    failure #135 exists to name)."""
    if not house:
        return None, ("no --manager given and no house could be inferred (no --house, no "
                      ".osiris pin, empty cwd name) — pass --manager explicitly, or --house, "
                      "or run this from inside a project directory")
    from src.orchestrator.seats import fleet_occupancy

    candidates = [s for s in await fleet_occupancy(pool) if s.get("house") == house]
    if not candidates:
        return None, (f"no seats exist in house {house!r} yet — mint-seat needs an existing "
                      "seat as manager-of-record even to start a brand-new house (crossing "
                      "into one always does); pass --manager naming any existing seat")
    if len(candidates) > 1:
        names = ", ".join(sorted(c["handle"] for c in candidates if c.get("handle")))
        return None, (f"{len(candidates)} seats in house {house!r} ({names}) — ambiguous, "
                      "name one explicitly with --manager")
    return candidates[0]["handle"], None


async def cmd_mint_seat(
    handle: str, *, manager: str | None, project: str | None, house: str | None,
    model: str | None, actor: str, adopt: bool = False, force: bool = False,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris mint-seat <handle> [--manager <seat>] [--project P] [--house H] [--model M]
    [--actor <who>] [--adopt] [--force] — the console-script door onto mintseat.mint_seat,
    the SAME function the mint_seat MCP tool wraps (no duplicated guard: the near-miss/
    cross-house/live-adopt refusals are exactly mint_seat's own, untouched here).

    A DIFFERENT SHAPE OF GAP than fold_project/charter_for/amend_practice's stale-tool-
    index class: mint_seat's own MCP tool has no `manager` parameter at all — it INFERS
    the manager from the CALLING agent's own held seat ("the calling seat is always the
    manager... minting into someone else's org is a console act, deliberately absent
    here", mint_seat's own docstring). A raw terminal has no mounted agent identity to
    infer from, so this door took `manager` explicitly at first — then dispatch 3678 (the
    operator's own "make the cli friendly") asked for that requirement inferred too, the
    same way `--actor` already is: when `manager` is omitted, `_infer_manager` looks for
    the SOLE seat in the target house (`_context_house`: --house, else the cwd's own
    .osiris pin, else the cwd's own name) and refuses loudly — never guesses — if that's
    zero or several. Closes the exact gap CLI.md's own house law names: an operator
    standing up a brand-new seat had no door but a hand-rolled `python -c` heredoc against
    the live DB — precisely what ruling 45b074bf bans.

    Prints the SAME next_step guidance the receipt already carries (mint_seat's own
    occupancy-aware line — vacant: `osiris launch <handle>`; occupied/cold: nothing
    needed) rather than a second, driftable copy of that advice."""
    from src.actions.core import Actions
    from src.orchestrator.mintseat import mint_seat as _mint_seat

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:mint-seat")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris mint-seat: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        if manager is None:
            ctx_house = _context_house(house)
            manager, infer_error = await _infer_manager(pool, ctx_house)
            if infer_error:
                print(f"osiris mint-seat: {infer_error}", file=sys.stderr)
                return 1
            assert manager is not None  # _infer_manager's own contract: error XOR manager
            print(f"osiris mint-seat: inferred --manager={manager!r} — the only seat in "
                  f"house {ctx_house!r}; pass --manager explicitly to override")
        kwargs: dict[str, Any] = {"intended_model": model} if model else {}
        out = await _mint_seat(Actions(pool), manager=manager, handle=handle, house=house,
                               project=project, actor=actor, adopt=adopt, force=force,
                               **kwargs)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris mint-seat: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"{'minted' if out['seat_minted'] else 'adopted'} {out['handle']} "
          f"({out['seat_id']}), house={out['house']}")
    office = out.get("office")
    if office:
        print(f"office: {office['office']} (pin {office['osiris_pin']}, orders "
              f"{office['standing_orders']}, charter {office['charter_file']})")
    print(f"model: {out['intended_model']}"
          + (" (stamped)" if out.get("intended_model_stamped") else ""))
    print(f"manager: {out['manager_seat_id']} ({out['managed_by']})")
    print(f"occupancy: {out['occupancy']} — {out['next_step']}")
    return 0


# --- new -----------------------------------------------------------------------------------------

async def cmd_new(
    handle: str, path: str | None, *, project: str | None, model: str | None,
    actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris new <handle> [path] [--project P] [--model M] [--actor <who>] — ONE
    command, no ceremony (dispatch 3685/3688, the operator's own "too much witchcraft to
    spawn a project... I'll remember 'osiris new' boom"): found a SELF-MANAGED seat —
    Ooblek's own real shape, read off its own dossier before this was built rather than
    assumed — a directory + `.osiris` pin for its own code workspace (created if absent,
    `path` defaults to `~/code/<handle>`), a Seat with NO `managed_by` edge ever, an
    office scaffold at the standard `~/.osiris/seats/<handle>/` location, and its tree
    bound to the workspace (`bind_seat_tree` — distinct from the office, offices.py's own
    "code stays in the repos they GOVERN"). The console-script door onto
    mintseat.found_seat, which composes mint_seat's OWN primitives (`ensure_seat`,
    `_scaffold_office`) rather than reimplementing them.

    Does not create a `governs` edge — the new seat charters itself, live, on its own
    first turn (its own compiled CLAUDE.md says so), matching Ooblek's real bootstrap
    order: self-claimed, then officed, then — once actually live — self-chartered.

    `osiris new` and `osiris launch` are the two commands meant to be memorized: found,
    then launch. Prints the exact `osiris launch <handle>` line so the second half never
    needs remembering either."""
    from src.actions.core import Actions
    from src.orchestrator.mintseat import found_seat as _found_seat

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:new")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris new: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        kwargs: dict[str, Any] = {"intended_model": model} if model else {}
        out = await _found_seat(Actions(pool), handle=handle, path=path, project=project,
                                actor=actor, **kwargs)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris new: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"{'founded' if out['seat_minted'] else 'converged on'} {out['handle']} "
          f"({out['seat_id']}) — self-managed, no manager")
    print(f"project: {out['project']}")
    print(f"workspace: {out['workspace']} ({out['workspace_pin']})")
    office = out.get("office")
    if office:
        print(f"office: {office['office']} (pin {office['osiris_pin']}, orders "
              f"{office['standing_orders']}, charter {office['charter_file']})")
    print(f"model: {out['intended_model']}"
          + (" (stamped)" if out.get("intended_model_stamped") else ""))
    print(f"occupancy: {out['occupancy']} — {out['next_step']}")
    print(f"next: osiris launch {out['handle']}")
    return 0


# --- bootstrap ---------------------------------------------------------------------------------

async def cmd_bootstrap(
    cwd: str, *, project: str | None, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris bootstrap <cwd> [--project P] [--actor <who>] — the console-script door
    onto bootstrap.bootstrap_project, the SAME function the `bootstrap` MCP tool wraps —
    same name, same first param name (`cwd`, matching the MCP tool's own signature
    exactly rather than a synonym like `path`, per the CLI/MCP parity law, decision
    0b29f1cbcc5a). #135 deliverable 3's last of three missing verbs (decision 3db8832c):
    a no-ctx, explicit-string-arg function, same shape as mint-seat/new's own CLI doors,
    undoored until now for no architectural reason.

    Migrates `cwd`'s markdown MEMORY (CLAUDE.md build log / DESIGN.md / memory essays)
    into the graph as retrieval-sized Reference nodes and registers the SoftwareProject —
    it does NOT touch the project's files (no hands); it prints a suggested boot-sector
    CLAUDE.md for a human or that project's own agent to review and write. `--actor`
    stamps every write this call makes (the registration and every ingested log entry),
    same default-to-console pattern as mint-seat/new (a raw terminal call already carries
    operator authority by construction — see `_CONSOLE_ACTOR`'s own comment). `--project`
    is CLI-only (declared in CLI_ONLY_PARAMS): bootstrap_project itself takes this
    override, the MCP tool's own wrapper simply never exposes it — a gap on that side,
    not an inconsistency to paper over here.

    THE LIVE-DB GUARD (9b9ba394, found live 2026-08-16 — this exact command wrote a
    real, if small, specimen into the shared fleet graph during its own verification):
    on this box `apply_dev_fallback()`'s "dev" DSN and every deployed service's own
    DATABASE_URL are the SAME database (`dev_env.py`'s own docstring assumes a
    separate `/etc/osiris/osiris.env` prod file that does not exist here — confirmed
    absent) — there is no isolated instance to fall back to. Unlike `merge`/`mint-seat`/
    `deploy`/etc. (deliberate operator acts a bare terminal call is SUPPOSED to run
    against the real graph), `bootstrap` is the one CLI door whose ordinary use includes
    exploratory/scratch runs — exactly the shape that produced the specimen. So: if the
    caller did not set DATABASE_URL themselves (about to hit the fallback) AND has not
    set OSIRIS_ALLOW_LIVE=1, this refuses loudly instead of writing to production by
    accident. An explicit DATABASE_URL (including one a deployed unit's own environment
    already carries) always wins and is never blocked.

    THE CHECK ITSELF NOW LIVES IN `dev_env.refuse_silent_live_db` (thread 86d562e0's own
    CLASS fix, not just this one door) — reused verbatim here, never a second copy."""
    from src.actions.core import Actions
    from src.orchestrator.bootstrap import bootstrap_project

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback, refuse_silent_live_db
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        refusal = refuse_silent_live_db("osiris bootstrap")
        if refusal is not None:
            print(refusal, file=sys.stderr)
            return 1
        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=4,
                application_name="osiris-cli:bootstrap")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris bootstrap: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        out = await bootstrap_project(Actions(pool), cwd, project=project, source=actor)
    finally:
        if owns_pool:
            await pool.close()
    print(f"project={out['project']} entries={out['entries']} ({out['registered']})")
    for i in out["ingested"]:
        print(f"  {i['file']:24} {i['entries']:>3} {i['as']}")
    print(out["note"])
    return 0


# --- argv dispatch -----------------------------------------------------------------------------

# dispatch 3678, "make the cli a front door instead of a dump": bare `osiris` used to be
# an argparse error (`the following arguments are required: command`) followed, on -h/
# --help, by a flat alphabetical dump of thirteen verbs with no sense of what a newcomer
# actually needs first. This is that front door — GROUPED by what a person is trying to
# DO (#97's own acceptance test: a stranger with no context gets from `osiris` to a
# running worker without reading source or asking anyone), with the newcomer path shown
# as literal copy-pasteable lines rather than prose describing it. Every individual
# subcommand's own `help=` text is suppressed from argparse's default listing (it would
# otherwise print AGAIN, alphabetically, right below this) — `osiris <verb> --help`
# still shows that verb's own full description and a worked example, untouched.
_TOP_LEVEL_HELP = """\
THE TWO COMMANDS TO REMEMBER — nothing to a working, independent mind:
    osiris new <name>
    osiris launch <name>
`new` founds a SELF-MANAGED seat (no manager, ever) with its own code workspace
(~/code/<name> by default) — a brand-new, independent project in the same act, no repo
required. `launch` bodies it. Nothing else to hold in memory; everything below is
discoverable when you need it, not something to remember in advance.

ADDING A WORKER TO A HOUSE YOU ALREADY RUN (a different case — MANAGED, not independent):
    osiris mint-seat <name>
    osiris launch <name>
(--manager/--actor are inferred — the seat you're standing in, the console actor — and
stay real overrides when that's wrong or ambiguous. Naming a --house with no seats in it
yet brings that house/project into existence in this same act too — --project defaults
to the house name.)

COMMANDS, GROUPED BY WHAT YOU'RE TRYING TO DO:
  start a mind          new, launch, mint-seat, attach
  end one               stop
  see the fleet         fleet, roster, boot-status, smoke
  read the record       desk, show
  write to the record   annotate-thread, amend-decision, charter-for, amend-practice,
                        merge, unmerge, fold-project
  operate               deploy, migrate, seed, bootstrap, retention, rematerialize

Every read verb takes --json: one compact line for a script or an agent, instead of the
human view. Run `osiris <command> --help` for that command's own flags and a worked example.
"""


class _RawSubparser(argparse.ArgumentParser):
    """Every subcommand's own --help gets RawDescriptionHelpFormatter too (not just the
    top level) — several epilogs below are literal copy-pasteable command lines, and the
    default formatter re-wraps/re-flows them, destroying exactly the thing they're for."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("formatter_class", argparse.RawDescriptionHelpFormatter)
        super().__init__(*args, **kwargs)


def _d(text: str) -> str:
    """Wrap a subcommand's own `description=` prose to a terminal-friendly width.
    RawDescriptionHelpFormatter (needed below so worked-example epilogs keep their literal
    line breaks) disables argparse's own wrapping for EVERYTHING on that parser, description
    included — without this, a description written as ordinary flowing prose renders as one
    unbroken line, exactly the "flat dump" this whole rebuild exists to fix."""
    return textwrap.fill(text, width=78)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="osiris", description=_TOP_LEVEL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=False, parser_class=_RawSubparser)

    p_attach = sub.add_parser("attach", description=_d("attach to a live seat's PTY session"),
                              epilog="example: osiris attach Khnum")
    p_attach.add_argument("handle")

    p_smoke = sub.add_parser(
        "smoke", description=_d("the same deploy-time liveness probe the fleet runs"),
        epilog="example: osiris smoke\nexample: osiris smoke --chaos")
    p_smoke.add_argument(
        "--chaos", action="store_true",
        help="crash replay (Thoth msg 5338): kill osiris-mcp/osiris-worker hard, fire a "
             "concurrent session-end storm, restart, then assert the #178 invariants hold "
             "under a real crash, never just a graceful restart")

    sub.add_parser("boot-status", description=_d(
        "name every active seat with no compiled managed section, "
                               "classified by why (report-only; exit 1 if any)"),
                   epilog="example: osiris boot-status")

    p_seed = sub.add_parser("seed", description=_d("seed default compositions (and rooms)"),
                            epilog="example: osiris seed\nexample: osiris seed "
                                   "--compositions-only")
    p_seed.add_argument("--compositions-only", action="store_true",
                        help="seed + room DEFAULT_COMPOSITIONS only; skip the canon ingest")

    p_launch = sub.add_parser("launch", description=_d("body a seat: spawn its claude process via "
                                          "`claude --bg` (task #72) so it lands in the "
                                          "operator's own `claude agents` list"),
                              epilog="example: osiris launch Khnum")
    p_launch.add_argument("handle")
    p_launch.add_argument("--model", default=None)
    p_launch.add_argument("--debug", action="store_true",
                          help="use the osiris PTY-broker lane instead of the default "
                               "`claude --bg` — for an incident or a build with no --bg")

    p_stop = sub.add_parser("stop", description=_d(
        "END a live body — `osiris launch`'s inverse. SIGTERMs the seat's own process and "
        "nothing more: not a pause, no promised thaw-where-you-left-off (ruling b3ccd3f6). "
        "Reachability afterward is governed by the SAME occupancy authority launch/wake "
        "already read, so a later launch just works — there is no 'unstop' to remember. "
        "`no-live-body` exits 0: nothing running there is a SUCCESS for a teardown"),
        epilog="example, ending a worker you started:\n"
               "    osiris stop Khnum --reason 'test run done'\n"
               "example, in a teardown loop (0 whether it was live or already gone):\n"
               "    osiris stop probe-seat || echo 'refused, see stderr'")
    p_stop.add_argument("handle", help="the seat handle whose body to end")
    p_stop.add_argument("--reason", default="",
                        help="recorded on the seat as stopped_reason — say why, for whoever "
                             "reads this later")
    p_stop.add_argument("--json", action="store_true", dest="as_json",
                        help="machine-readable: one compact JSON line")

    p_fleet = sub.add_parser("fleet", description=_d(
        "the fleet roster, grouped by project — the same "
                                         "fleet() the MCP tool answers, called over the wire"),
                             epilog="example: osiris fleet\nexample: osiris fleet --full")
    p_fleet.add_argument("--full", action="store_true")
    p_fleet.add_argument("--json", action="store_true", dest="as_json",
                         help="machine-readable: one compact JSON line, for a script or an agent")

    p_roster = sub.add_parser("roster", description=_d(
        "which seat owns a repo, and is anybody home — the same roster() the MCP tool "
        "answers, called over the wire. Without --repo: every active seat's occupancy "
        "(vacant/occupied/cold — cold is NOT vacant), charter, and .osiris pin. With "
        "--repo: which seat's charter or pin names it, flagged if they disagree"),
        epilog="example: osiris roster\nexample: osiris roster --repo coldspot")
    p_roster.add_argument("--repo", default=None,
                          help="reverse-lookup: which seat owns this repo")
    p_roster.add_argument("--json", action="store_true", dest="as_json",
                          help="machine-readable: one compact JSON line, for a script or an agent")

    p_desk = sub.add_parser("desk", description=_d(
        "the operator's own organized queue — needs_decision / needs_hands / fyi bands, "
        "the your_queue thread list, dimmed moot briefs — read at a terminal instead of "
        "only the web console or an agent peeking on your behalf. Always a peek; settling "
        "a brief is still only ever your own explicit word (the desk MCP tool's ack=)"),
        epilog="example: osiris desk\nexample: osiris desk --json")
    p_desk.add_argument("--json", action="store_true", dest="as_json",
                        help="machine-readable: one compact JSON line, for a script or an agent")

    p_show = sub.add_parser("show", description=_d(
        "the full, untruncated record for one Thread or Decision — by UUID, 8-char short "
        "id, or summary substring, the same recall() an agent already reads. Refuses "
        "loudly (never guesses) when nothing matches"),
        epilog="example: osiris show 5f234a1c\nexample: osiris show 5f234a1c --json")
    p_show.add_argument("ref", help="UUID, 8-char short id, or summary substring")
    p_show.add_argument("--json", action="store_true", dest="as_json",
                        help="machine-readable: one compact JSON line, for a script or an agent")

    p_migrate = sub.add_parser("migrate", description=_d(
        "env-correct `alembic upgrade head` (--check "
                                           "reports without applying)"),
                               epilog="example: osiris migrate --check")
    p_migrate.add_argument("--check", action="store_true",
                           help="report a pending revision without applying it")

    sub.add_parser("deploy", description=_d("the deploy ritual as one verb: dirty-guard, migrate, "
                               "restart, smoke, un-run-step report"),
                   epilog="example: osiris deploy")

    p_merge = sub.add_parser(
        "merge", description=_d("Fold `dupe` into `into` — declare two labels of the SAME "
            "type one thing (Agent, Seat, or SoftwareProject; self-typing off dupe's own "
            "form: agent:.../seat:.../else). Append-only (a merge event, nothing "
            "deleted); each type's own estate (mail, mounts, threads, holders, edges) "
            "follows onto the survivor. The same orchestrator.merge.merge the MCP tool "
            "wraps — replaces the old `fold-project` name (kept as a deprecated, working "
            "alias for a SoftwareProject-only call)."),
        epilog="example: osiris merge OldLabel NewLabel --evidence \"same repo, two "
            "labels\"")
    p_merge.add_argument("dupe", help="the duplicate label — agent:/seat: prefix picks "
                         "that type, anything else means SoftwareProject")
    p_merge.add_argument("into", help="the surviving label, same type as dupe")
    p_merge.add_argument("--evidence", required=True,
                         help="why these are one thing, not two")
    p_merge.add_argument("--actor", default=_CONSOLE_ACTOR,
                         help="who is performing this merge — defaults to "
                              f"{_CONSOLE_ACTOR!r} (a terminal call already carries "
                              "operator authority, the only gate an Agent merge enforces)")

    p_unmerge = sub.add_parser(
        "unmerge", description=_d("Reverse a wrongful `merge` — dry run by default (returns "
            "the reversal plan, writes nothing); pass --execute once you've reviewed it. "
            "Self-typing off dupe's own form, same rule as merge. The same "
            "orchestrator.merge.unmerge the MCP tool wraps."),
        epilog="example, review the plan first:\n"
            "    osiris unmerge OldLabel --because \"was never actually a duplicate\"\n"
            "example, then apply it:\n"
            "    osiris unmerge OldLabel --because \"was never actually a duplicate\" "
            "--execute")
    p_unmerge.add_argument("dupe", help="the previously-merged label to un-fold")
    p_unmerge.add_argument("--because", required=True,
                           help="why this merge is being reversed")
    p_unmerge.add_argument("--actor", default=_CONSOLE_ACTOR,
                           help=f"who is reversing this merge — defaults to "
                                f"{_CONSOLE_ACTOR!r}")
    p_unmerge.add_argument("--execute", action="store_true",
                           help="apply the reversal plan instead of only showing it")
    p_unmerge.add_argument("--json", action="store_true", dest="as_json",
                           help="machine-readable: one compact JSON line, for a script or an agent")

    p_retention = sub.add_parser(
        "retention", description=_d("Prune outbox/audit_log rows past their retention "
            "window — dry run by default (counts only, writes nothing); pass --execute "
            "to delete, in batches. Thread e6fd3772 piece 1."),
        epilog="example, count only:\n"
            "    osiris retention outbox\n"
            "example, then delete:\n"
            "    osiris retention outbox --execute")
    p_retention.add_argument("table", choices=["outbox", "audit-log"],
                             help="which table's retention to run")
    p_retention.add_argument("--days", type=int, default=None,
                             help="retention window in days (default: outbox 30, "
                                  "audit-log 90)")
    p_retention.add_argument("--execute", action="store_true",
                             help="delete the eligible rows instead of only counting them")
    p_retention.add_argument("--batch-size", type=int, default=5000,
                             help="rows deleted per statement when --execute (default 5000)")
    p_retention.add_argument("--json", action="store_true", dest="as_json",
                             help="machine-readable: one compact JSON line")

    # DEPRECATED (dispatch 3683): fold_project no longer exists as an MCP tool — see
    # cmd_fold_project's own docstring. Kept working, hidden from the front-door listing
    # (no help= means argparse's own choice listing never mentions it either) — never
    # break a human's muscle memory silently, but never advertise the old name again.
    p_fold_project = sub.add_parser(
        "fold-project",
        description=_d("DEPRECATED — use `osiris merge` instead (identical arguments, same "
            "evidence-gated fold). Kept working for muscle memory; never advertised."),
        epilog="example: osiris fold-project OldLabel NewLabel --evidence "
            "\"same repo, two labels\"")
    p_fold_project.add_argument("dupe", help="the duplicate project's label")
    p_fold_project.add_argument("into", help="the surviving project's label")
    p_fold_project.add_argument("--evidence", required=True,
                                help="why these are one project, not two")
    p_fold_project.add_argument("--actor", default=_CONSOLE_ACTOR,
                                help="who is performing this fold — defaults to "
                                     f"{_CONSOLE_ACTOR!r} (a terminal call already carries "
                                     "operator authority); override to attribute it "
                                     "elsewhere")

    p_charter_for = sub.add_parser("charter-for", description=_d(
        "declare a charter on behalf of a seat — the "
                                       "same manager/operator-enforced charter_for the MCP "
                                       "tool wraps, exposed as the sanctioned second door"),
                                   epilog="example: osiris charter-for seat:a1b2c3d4 "
                                       "--repos osiris,osiris-console "
                                       "--because \"declared on the seat's own behalf\"")
    p_charter_for.add_argument("seat", help="the target seat's canonical (seat:<id>)")
    p_charter_for.add_argument("--repos", required=True,
                               help="comma-separated repo labels — the whole charter, not "
                                    "an increment")
    p_charter_for.add_argument("--because", required=True,
                               help="why this charter is being declared on the seat's behalf")
    p_charter_for.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help="who is declaring this charter — must be the seat's "
                                    f"manager or an operator actor; defaults to {_CONSOLE_ACTOR!r} "
                                    "(already an operator actor)")

    p_amend_practice = sub.add_parser("amend-practice", description=_d(
        "narrow or correct a LIVE practice's "
                                          "guidance — the same amend_practice the MCP tool "
                                          "wraps, exposed as the sanctioned second door"),
                                      epilog="example: osiris amend-practice a1b2c3d4 "
                                          "\"except when the target is a fresh clone\"")
    p_amend_practice.add_argument("ref", help="the target practice's uuid, canonical, "
                                  "short-id prefix, or statement substring")
    p_amend_practice.add_argument("amendment", help="the text to add — never replaces the "
                                  "practice's own statement")
    p_amend_practice.add_argument("--actor", default=_CONSOLE_ACTOR,
                                  help=f"who is making this amendment — defaults to "
                                       f"{_CONSOLE_ACTOR!r}")

    p_annotate_thread = sub.add_parser("annotate-thread", description=_d(
        "add to a thread's record without "
                                           "closing it — the same annotate_thread the MCP "
                                           "tool wraps, exposed as the sanctioned second door"),
                                       epilog="example: osiris annotate-thread a1b2c3d4 "
                                           "\"confirmed independently, see commit abc1234\"")
    p_annotate_thread.add_argument("ref", help="the target thread's uuid, canonical, "
                                   "short-id prefix, or summary substring")
    p_annotate_thread.add_argument("note", help="the note to append — never touches "
                                   "summary/status")
    p_annotate_thread.add_argument("--actor", default=_CONSOLE_ACTOR,
                                   help=f"who is adding this note — defaults to "
                                        f"{_CONSOLE_ACTOR!r}")

    p_amend_decision = sub.add_parser("amend-decision", description=_d(
        "append reasoning to a LIVE decision "
                                          "without superseding it — the same amend_decision "
                                          "the MCP tool wraps, exposed as the sanctioned "
                                          "second door"),
                                      epilog="example: osiris amend-decision a1b2c3d4 "
                                          "\"the smaller residual specimen still held up\"")
    p_amend_decision.add_argument("ref", help="the target decision's uuid, canonical, "
                                  "short-id prefix, or summary substring")
    p_amend_decision.add_argument("addendum", help="the text to add — never replaces the "
                                  "decision's own summary/rationale/kind")
    p_amend_decision.add_argument("--actor", default=_CONSOLE_ACTOR,
                                  help=f"who is making this addendum — defaults to "
                                       f"{_CONSOLE_ACTOR!r}")

    p_rematerialize = sub.add_parser(
        "rematerialize", description=_d(
            "reconstruct a session's transcript BYTE-FOR-BYTE from the soul store's "
            "soul_lines alone (task #51 piece 2) — the same SoulStore.rematerialize_to_"
            "disk the MCP tool wraps. Verifies the hash chain while collecting; a break "
            "is reported and NOTHING is written, never a silent partial file. Refuses "
            "to overwrite a transcript modified more recently than the store's last "
            "ingest unless --force is given."),
        epilog="example: osiris rematerialize deadbeef\n"
            "example, to a specific path: osiris rematerialize deadbeef "
            "--dest /tmp/recovered.jsonl")
    p_rematerialize.add_argument("anchor_sid", help="the 8-char session anchor to "
                                 "reconstruct")
    p_rematerialize.add_argument("--dest", default=None,
                                 help="where to write the reconstruction — defaults to "
                                      "the session's own recorded source_path (the "
                                      "harness's own projects-slug convention)")
    p_rematerialize.add_argument("--force", action="store_true",
                                 help="write even if the target exists and was modified "
                                      "more recently than the store's last ingest")

    p_mint_seat = sub.add_parser(
        "mint-seat", description=_d(
            "Mint (or adopt) a worker seat: ensure_seat + an office scaffold on "
            "disk (a directory, an .osiris pin carrying project AND model, CLAUDE.md, "
            "charter.md) + an intended_model stamp + a managed_by link to the manager. "
            "Idempotent — a handle that already names a living seat is ADOPTED (missing "
            "pieces filled in, nothing rewritten) rather than twinned. The same "
            "mintseat.mint_seat the MCP tool wraps; --manager/--actor are CLI-only (a raw "
            "terminal holds no seat of its own to infer them from, so this door infers "
            "them a different way — see their own --help below) and --adopt/--force are "
            "deliberate console-only escape hatches an agent caller can never reach."),
        epilog="example, adding a worker to your own house:\n"
            "    osiris mint-seat NewBot\n"
            "example, starting a brand-new house/project:\n"
            "    osiris mint-seat NewBot --manager Thoth --house NewProject")
    p_mint_seat.add_argument("handle", help="the new worker seat's handle")
    p_mint_seat.add_argument("--manager", default=None,
                             help="the minting seat's own handle or seat_id. Omit it and "
                                  "this infers the sole seat in the target house (--house, "
                                  "else the cwd's .osiris pin, else the cwd's own name) — "
                                  "refuses loudly instead of guessing among several")
    p_mint_seat.add_argument("--project", default=None,
                             help="the project the new seat's office is stamped with "
                                  "(defaults to the manager's own house)")
    p_mint_seat.add_argument("--house", default=None,
                             help="defaults to the manager's own house; naming a house with "
                                  "no seats in it yet CREATES that house/project in this "
                                  "same act (a console actor already carries the operator "
                                  "authority a house crossing needs)")
    p_mint_seat.add_argument("--model", default=None,
                             help="defaults to mint_seat's own worker default")
    p_mint_seat.add_argument("--actor", default=_CONSOLE_ACTOR,
                             help=f"who is performing this mint — defaults to "
                                  f"{_CONSOLE_ACTOR!r}")
    p_mint_seat.add_argument("--adopt", action="store_true",
                             help="state explicitly that handle names an EXISTING seat to "
                                  "adopt — refuses instead of silently minting fresh on no "
                                  "match")
    p_mint_seat.add_argument("--force", action="store_true",
                             help="mint a distinct seat past a near-miss handle refusal")

    p_new = sub.add_parser(
        "new", description=_d(
            "Found a SELF-MANAGED seat: no manager, ever (Ooblek's own real shape, "
            "read off its dossier, never a flag). One act: create/verify a code "
            "workspace directory + its own .osiris pin, mint the seat, scaffold its "
            "identity office at the standard ~/.osiris/seats/<handle>/ location, and "
            "bind its tree to the workspace. Does not charter the seat over a "
            "project — it charters itself, live, on its own first turn."),
        epilog="example, converging on ~/code/henry:\n"
            "    osiris new henry\n"
            "example, naming the workspace explicitly:\n"
            "    osiris new henry ~/projects/henry-thing\n"
            "then:\n"
            "    osiris launch henry")
    p_new.add_argument("handle", help="the new self-managed seat's handle")
    p_new.add_argument("path", nargs="?", default=None,
                       help="the code workspace directory (created if absent) — "
                            "defaults to ~/code/<handle>")
    p_new.add_argument("--project", default=None,
                       help="the project name written into the workspace's own .osiris "
                            "pin — defaults to the handle")
    p_new.add_argument("--model", default=None,
                       help="defaults to mint_seat's own worker default")
    p_new.add_argument("--actor", default=_CONSOLE_ACTOR,
                       help=f"who is performing this act — defaults to {_CONSOLE_ACTOR!r}")

    p_bootstrap = sub.add_parser(
        "bootstrap", description=_d(
            "Onboard an EXISTING project: migrate its markdown memory (CLAUDE.md build "
            "log / DESIGN.md / memory essays) into the graph as retrieval-sized "
            "Reference nodes and register it. No hands on the project's files — reads "
            "the mds, writes the graph, prints a suggested boot-sector CLAUDE.md for a "
            "human to review and write. Different from `new`: this does not mint a seat "
            "or touch identity, it only brings a project's knowledge into the graph."),
        epilog="example:\n"
            "    osiris bootstrap ~/code/some-project\n"
            "example, naming the project explicitly:\n"
            "    osiris bootstrap ~/code/some-project --project some-project")
    p_bootstrap.add_argument("cwd", help="the project's directory on disk")
    p_bootstrap.add_argument("--project", default=None,
                             help="defaults to the directory's own basename — CLI-only, "
                                  "the MCP tool always infers it")
    p_bootstrap.add_argument("--actor", default=_CONSOLE_ACTOR,
                             help=f"who is performing this act — defaults to "
                                  f"{_CONSOLE_ACTOR!r}")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        # dispatch 3678/3681: bare `osiris` used to be argparse's own terse usage error.
        # Thoth's own measurement says the EXIT CODE (2, a real usage condition — no
        # command was given) was already correct and must stay; only the TEXT was the
        # dump. print_help() shows the full front-door description above; the code stays 2.
        parser.print_help()
        return 2
    if args.command == "attach":
        return asyncio.run(cmd_attach(args.handle))
    if args.command == "smoke":
        return asyncio.run(cmd_smoke(chaos=args.chaos))
    if args.command == "boot-status":
        return asyncio.run(cmd_boot_status())
    if args.command == "seed":
        return asyncio.run(cmd_seed(compositions_only=args.compositions_only))
    if args.command == "launch":
        return asyncio.run(cmd_launch(args.handle, model=args.model, debug=args.debug))
    if args.command == "stop":
        return asyncio.run(cmd_stop(args.handle, reason=args.reason, as_json=args.as_json))
    if args.command == "fleet":
        return asyncio.run(cmd_fleet(full=args.full, as_json=args.as_json))
    if args.command == "roster":
        return asyncio.run(cmd_roster(repo=args.repo, as_json=args.as_json))
    if args.command == "desk":
        return asyncio.run(cmd_desk(as_json=args.as_json))
    if args.command == "show":
        return asyncio.run(cmd_show(args.ref, as_json=args.as_json))
    if args.command == "migrate":
        return asyncio.run(cmd_migrate(check=args.check))
    if args.command == "deploy":
        return asyncio.run(cmd_deploy())
    if args.command == "merge":
        return asyncio.run(cmd_merge(args.dupe, args.into, args.evidence, actor=args.actor))
    if args.command == "unmerge":
        return asyncio.run(cmd_unmerge(args.dupe, args.because, actor=args.actor,
                                       execute=args.execute, as_json=args.as_json))
    if args.command == "retention":
        return asyncio.run(cmd_retention(args.table, days=args.days, execute=args.execute,
                                         batch_size=args.batch_size,
                                         as_json=args.as_json))
    if args.command == "fold-project":
        return asyncio.run(cmd_fold_project(args.dupe, args.into, args.evidence,
                                            actor=args.actor))
    if args.command == "charter-for":
        repos = [r.strip() for r in args.repos.split(",") if r.strip()]
        return asyncio.run(cmd_charter_for(args.seat, repos, args.because, actor=args.actor))
    if args.command == "amend-practice":
        return asyncio.run(cmd_amend_practice(args.ref, args.amendment, actor=args.actor))
    if args.command == "annotate-thread":
        return asyncio.run(cmd_annotate_thread(args.ref, args.note, actor=args.actor))
    if args.command == "amend-decision":
        return asyncio.run(cmd_amend_decision(args.ref, args.addendum, actor=args.actor))
    if args.command == "rematerialize":
        return asyncio.run(cmd_rematerialize(args.anchor_sid, dest=args.dest,
                                             force=args.force))
    if args.command == "mint-seat":
        return asyncio.run(cmd_mint_seat(
            args.handle, manager=args.manager, project=args.project, house=args.house,
            model=args.model, actor=args.actor, adopt=args.adopt, force=args.force))
    if args.command == "new":
        return asyncio.run(cmd_new(
            args.handle, args.path, project=args.project, model=args.model,
            actor=args.actor))
    if args.command == "bootstrap":
        return asyncio.run(cmd_bootstrap(args.cwd, project=args.project, actor=args.actor))
    return 2  # pragma: no cover - every real subparser choice is handled above; argparse
    # itself refuses anything not in `sub.choices`, so this is unreachable in practice


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
