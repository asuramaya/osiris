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
ClearStaleRecord = Callable[..., Awaitable[bool]]

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


def match_session(sessions: list[dict[str, Any]], handle: str) -> tuple[str | None, list[str]]:
    """(name, candidates). `name` is set only for an unambiguous match against the manager
    daemon's own pty_list roster; `candidates` lists every session name that matched loosely,
    for an honest disambiguation message when `name` is None. A handle is matched against the
    TAIL of the window's own '[TAG] Handle' name (trigger.py's _window_name convention) so a
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


async def cmd_smoke(*, chaos: bool = False, as_json: bool = False) -> int:
    """`as_json` (Thoth dispatch 6746, specimen A): never chaos's own path — `--chaos`
    runs a longer-lived, separate probe (`cmd_smoke_chaos`) with its own text-only
    receipt, untouched here; the ordinary probe's `fails`/`warnings` are already
    `list[str]`, trivially JSON-serializable."""
    if chaos:
        return await cmd_smoke_chaos()
    fails, warnings = await _run_smoke_probes_full()
    if as_json:
        from src import cli_render as render
        render.emit({"fails": fails, "warnings": warnings}, as_json=True)
        return 0 if not fails else 1
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

async def cmd_boot_status(*, pool: asyncpg.Pool | None = None, as_json: bool = False) -> int:
    """Report-only rollout check (thread 0e5bae06, #84) — names every active seat NOT
    carrying a compiled managed section, classified by why, same shape as
    `composition_gap_notes`: a build isn't done when its acceptance test passes, it's
    done when the effect reaches every office, and 'reached most of them' is a gap this
    prints by name, never a count that can read clean while 19 offices are silently
    unreached.

    `as_json` (Thoth dispatch 6746, specimen A): the CLI's own top-level help claims
    "Every read verb takes --json" for this verb's own displayed group — this used to be
    false. `gaps` is already `list[dict[str, str]]`, trivially JSON-serializable; no
    second data shape invented for the machine path."""
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
    if as_json:
        from src import cli_render as render
        render.emit({"gaps": gaps}, as_json=True)
        return 1 if gaps else 0
    if not gaps:
        print("boot: every active seat carries a compiled managed section")
        return 0
    for note in boot_rollout_gap_notes(gaps):
        print(note)
    return 1


# --- lint --------------------------------------------------------------------------------------

def _lint_project_match(finding: dict[str, Any], project: str) -> bool:
    """A crude, honest heuristic (WAVE 22 scope note, thread bf10608b): graph_lint's own
    findings carry no dedicated project/repo field at all (unlike closure_health's own
    args.repo scoping) — most checks are fleet-wide graph-integrity questions with no
    single project to attribute a finding to. Rather than invent a false-precision per-
    check SQL join graph_lint itself doesn't have, this matches `project` as a case-
    insensitive substring of the two fields nearly every check actually populates
    (`subject`, `detail`) — real but approximate, documented as such in the receipt via
    `checks_not_evaluable_for_project`, never silently presented as a genuine scope."""
    needle = project.lower()
    for key in ("subject", "detail"):
        val = finding.get(key)
        if isinstance(val, str) and needle in val.lower():
            return True
    return False


async def cmd_lint(
    *, check: str | None = None, project: str | None = None, as_json: bool = False,
    stale_days: int = 14, limit: int | None = None, offset: int = 0,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris lint [--check NAME] [--project P] [--json] — the console-script door onto
    graph_lint's own orchestrator function, the SAME call graph_lint's MCP tool makes
    (mcp_server.py:1379: `comp.run_spec(pool, {"op": "function", "name": "lint", ...},
    None, name="graph-lint")`) — never a second implementation of the 32 checks
    themselves. Headless health check for a cron or a stranger's shell with no MCP
    client: exit 0 when the (optionally --check/--project-scoped) result carries zero
    findings, 1 when it carries any.

    `--project P` is a CLIENT-SIDE post-filter — see `_lint_project_match`'s own
    docstring for why (WAVE 22 scope note, thread bf10608b): graph_lint has no per-check
    SQL-level project scoping to mirror, so inventing one here would be new capability,
    not a mirror. The receipt names which checks' findings could not even be evaluated
    for project membership (most of them), rather than silently implying a scoped-clean
    pass just because nothing matched."""
    from src.orchestrator import compositions as comp

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
                application_name="osiris-cli:lint")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris lint: could not reach postgres at {settings.database_url} — "
                  f"{exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    args: dict[str, Any] = {"stale_days": stale_days}
    if check is not None:
        args["check"] = check
    if limit is not None:
        args["limit"] = limit
    if offset:
        args["offset"] = offset
    spec = {"op": "function", "name": "lint", "args": args}
    try:
        out = await comp.run_spec(pool, spec, None, name="graph-lint")
    finally:
        if owns_pool:
            await pool.close()
    items: dict[str, Any] = out["items"]
    findings: list[dict[str, Any]] = items.get("findings", [])

    if project:
        scoped = [f for f in findings if _lint_project_match(f, project)]
        checked_names = {f.get("check") for f in findings}
        matched_names = {f.get("check") for f in scoped}
        items = {**items, "findings": scoped,
                "project_filter": {
                    "project": project,
                    "checks_not_evaluable_for_project": sorted(checked_names - matched_names)}}
        findings = scoped

    if as_json:
        from src import cli_render as render
        render.emit(items, as_json=True)
        return 1 if findings else 0

    if not findings:
        scope = f" for project {project!r}" if project else ""
        print(f"osiris lint: clean{scope} — no findings")
        return 0
    for f in findings:
        detail = f.get("detail", "")
        subject = f.get("subject")
        subj_part = f" ({subject})" if subject else ""
        print(f"[{f.get('severity', '?')}] {f.get('check', '?')}{subj_part}: {detail}")
    counts = items.get("counts", {})
    total = sum(counts.get(c, 0) for c in {f.get("check") for f in findings})
    print(f"osiris lint: {len(findings)} shown, {total} total in the listed checks — "
          f"see counts_by_severity/could_not_evaluate with --json for the full receipt")
    return 1


# --- audit -------------------------------------------------------------------------------------

# The 5 audit-shaped siblings graph_lint keeps beside it in the CMD-K palette (WAVE 22 scope
# note, thread bf10608b — console.js's own POWER_TOOLS, lines 840-857), EXCLUDING graph_lint
# itself (its own door, `osiris lint`) and the palette's non-audit analysis tools (Who Is This/
# Co-Investment Ties/Screen Financing/Op vs Disclosed Geo/LAP/Overhead/Echoes — reporting lenses,
# not health checks). Each name is a DEFAULT_COMPOSITIONS entry already, resolved by
# `comp.run_composition` exactly as the `composition(action='run')` MCP door resolves it.
AUDIT_NAMES: tuple[str, ...] = (
    "closure-health", "the-wall", "type-census", "family-consistency", "family-drift",
)


async def cmd_audit(
    name: str, *, as_json: bool = False, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris audit <name> [--json] — one console-script door onto graph_lint's audit
    siblings (see `AUDIT_NAMES`), calling `comp.run_composition` directly, the SAME
    function the `composition(action='run', name=...)` MCP door calls (mcp_server.py's
    `_composition_impl`) — never a second implementation, and never one subcommand per
    audit (five near-identical CLI doors drifting independently is exactly the class of
    bug graph_lint's own history (#48) already taught this house to avoid).

    No subject binding (every one of these five runs fleet-wide, matching what the CMD-K
    palette itself invokes — `runTool(name)` with no subject argument). Exit 1 only when
    the composition itself reports an error (an unknown name, a query failure); these are
    read-only lenses, not a pass/fail gate like `osiris lint` — a clean run and a run
    surfacing real findings both exit 0, the same way the palette itself never turns red
    on content."""
    from src.orchestrator import compositions as comp

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
                application_name="osiris-cli:audit")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris audit: could not reach postgres at {settings.database_url} — "
                  f"{exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        out = await comp.run_composition(pool, name, None)
    finally:
        if owns_pool:
            await pool.close()
    from src import cli_render as render
    render.emit(out, as_json=as_json)
    return 1 if isinstance(out, dict) and "error" in out else 0


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


async def _resolve_launch_target(
    pool: asyncpg.Pool, handle: str, *, verb: str = "launch",
) -> dict[str, Any] | None:
    """Handle -> seat facts (with `seat_id` folded in), or None with an honest stderr message
    already printed. Shared by launch AND resume (thread 60c78788, the operator's verb
    split) — the seat lookup and its error cases don't change with the door, only what
    happens once a target is found. `verb` names the actual caller in every printed line
    ('launch' or 'resume') so one function serves both without a second copy."""
    from src.orchestrator.seats import seat_facts, seats_by_handle

    seat_ids = await seats_by_handle(pool, handle)
    if not seat_ids:
        print(f"osiris {verb}: no living Seat holds handle {handle!r}.", file=sys.stderr)
        return None
    if len(seat_ids) > 1:
        print(f"osiris {verb}: {handle!r} is ambiguous — {len(seat_ids)} seats share it: "
              f"{seat_ids}. Use a more specific handle.", file=sys.stderr)
        return None
    facts = await seat_facts(pool, seat_ids[0])
    if not facts["handle"]:
        print(f"osiris {verb}: {seat_ids[0]} carries no handle assertion — a body cannot "
              "be named for a nameless seat.", file=sys.stderr)
        return None
    if not facts["anchor_cwd"]:
        print(f"osiris {verb}: {facts['handle']} ({seat_ids[0]}) has no anchor_cwd — "
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


async def _resolve_and_guard_launch(
    handle: str, *, pool: asyncpg.Pool, agents_json: AgentsJson, verb: str,
) -> tuple[dict[str, Any], str] | int:
    """`osiris resume`'s OWN GUARDS NOW (thread 60c78788, the operator's verb split —
    `osiris launch` always mints fresh, `osiris resume` always continues, no flag, no
    automatic guess — the VERB is the property): resolve the seat, refuse a missing
    anchor_cwd/tree_cwd BY NAME with the exact remedy (decision 27259e4d, thread
    bc11a2d3), and refuse (as a SUCCESS, exit 0) when a body already holds this seat.

    NO LONGER SHARED WITH `osiris launch` (WAVE 21 item 3, a793b01b, "UNIFY LAUNCH"):
    `_cmd_launch_harness` now calls `trigger.launch_seat` directly instead of this
    function — its own docstring names which of these guards it kept CLI-side and why.
    This function's own name and `verb` parameter are left as-is (still meaningful for
    resume's one remaining caller, and renaming risks a drive-by rewrite of a working,
    tested function for cosmetics alone). `verb` names the actual caller in every printed
    line so this stays ONE
    implementation, never a second copy drifting from the first (#48's own lesson).
    Returns `(facts, launch_cwd)` to proceed, or an int to return immediately."""
    from src.orchestrator.trigger import (
        _launch_twin_check,
        _tree_exists,
        fabricated_tree_verdict,
    )

    facts = await _resolve_launch_target(pool, handle, verb=verb)
    if facts is None:
        return 1
    office = facts["anchor_cwd"]
    tree_cwd = facts["tree_cwd"]
    # THE ONE-SIDED GUARD FAMILY, NEXT SPECIMEN (decision 27259e4d, thread bc11a2d3):
    # tree_cwd got an existence check and a named refusal below; office/anchor_cwd got
    # neither, so a stale or wrong anchor (pin drift, a renamed directory) flowed straight
    # into create_subprocess_exec(cwd=...) as a raw, uncaught FileNotFoundError. Checked
    # UNCONDITIONALLY, not just in the tree_cwd-absent fallback branch below: office is
    # ALSO the identity `_bg_boot_prompt` hands the spawned session for its own mount(cwd=)
    # call, regardless of which path becomes the subprocess's actual OS cwd — a bad anchor
    # is a real problem even when tree_cwd happens to save this call's own subprocess spawn.
    # NEVER derived-by-convention or silently repointed (Thoth's own instruction): which of
    # anchor_cwd/the on-disk path is the truth is an operator decision, not a guess either
    # side should make — this only refuses loudly and names which field disagrees.
    if not _tree_exists(office):
        print(f"osiris {verb}: {handle!r} names anchor_cwd={office!r} but it does not "
              "exist on disk — repoint it or create the directory before launch; osiris "
              "never provisions one itself. The anchor is a GRAPH assertion (not a "
              f".osiris pin file) — fix it with: rebind_seat(seat={handle!r}, "
              "new_cwd='<the real directory>') via the osiris MCP tools, or by creating "
              f"{office!r} at that exact path.", file=sys.stderr)
        return 1
    launch_cwd = office
    if tree_cwd:
        if not _tree_exists(tree_cwd):
            print(f"osiris {verb}: {handle!r} names tree_cwd={tree_cwd!r} but it does not "
                  "exist on disk — osiris expects the harness (or a human, via "
                  "EnterWorktree) to have created it before launch; it never provisions "
                  "one itself. Fix it with: bind_seat_tree(seat_id="
                  f"{facts['seat_id']!r}, tree_cwd='<the real directory>', because='...') "
                  "via the osiris MCP tools, or by creating it at that exact path.",
                  file=sys.stderr)
            return 1
        # THE #199 FABRICATION (operator, 2026-09-03: "launch lands the agent in the wrong
        # cwd"): a mint-time ~/code/<handle> with no git tree while the charter governs a
        # real tree elsewhere. Same check launch_seat runs (one implementation, 983ec87a);
        # refused BY NAME with the one-line remedy, never silently repointed.
        fabricated = await fabricated_tree_verdict(pool, facts["seat_id"], tree_cwd)
        if fabricated is not None:
            repo, real_path = fabricated
            print(f"osiris {verb}: {handle!r} names tree_cwd={tree_cwd!r}, which holds no "
                  f"git tree, while its charter governs {repo!r} at {real_path!r} (a real "
                  "tree) — the #199 mint-time fabrication; a body spawned there lands in "
                  "the wrong cwd. Fix it with: bind_seat_tree(seat_id="
                  f"{facts['seat_id']!r}, tree_cwd={real_path!r}, because='...') via the "
                  "osiris MCP tools.", file=sys.stderr)
            return 1
        launch_cwd = tree_cwd

    # THE SHARED TWIN GUARD (task #148's contested seam 4, ruling 983ec87a "two doors, one
    # receipt"): reads BOTH claude agents --json (the harness's own, known-incomplete roster
    # — invisible to a resumed non-bg body by construction) AND agent_mounts (osiris's own
    # registry, which a resumed body's mid-turn mount() call DOES reach), same helper
    # launch_seat's own harness-native lane calls, so the two doors can never drift.
    twin = await _launch_twin_check(pool, agents_json, launch_cwd, seat_id=facts["seat_id"])
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
        print(f"osiris {verb}: seen via {', '.join(seen_via)}", file=sys.stderr)
        return 0
    return facts, launch_cwd


async def _cmd_launch_harness(
    handle: str, *, model: str | None, pool: asyncpg.Pool, wake_default: str | None,
    spawn: SpawnClaudeBg, agents_json: AgentsJson,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    """THE DEFAULT LANE (task #72, following trigger.launch_seat's own flip, rulings 0fe36e59
    + 33d6a2eb clause 3): `claude --bg` + `claude agents --json`, the harness's own front-end
    surface — every body this creates is visible in the operator's own `claude agents` list
    BY CONSTRUCTION.

    NO LONGER A SEPARATE IMPLEMENTATION (WAVE 21 item 3, mail 9869 a793b01b, "UNIFY LAUNCH",
    closing #48's "two doors, one receipt" lesson for launch itself, not just its
    sub-pieces): this door now calls `trigger.launch_seat` directly, with
    `operator_authorized=True` — the ONE flag only this CLI's own local-execution trust
    boundary can set (see `launch_seat`'s own docstring). `wake_default` stays in the
    signature for the two tests that call this function directly by keyword, but is no
    longer read here: launch_seat's own `_resolve_launch_model` already resolves the same
    precedence (explicit -> stamped intended_model -> last holder's own source_model ->
    the trigger's global default) from the `settings` this call passes through, one tier
    RICHER than this door's own former `resolve_model` call ever was.

    ONE GUARD STAYS CLI-SIDE, ON PURPOSE (decision 27259e4d, thread bc11a2d3): office/
    anchor_cwd existing ON DISK. Porting it into the shared `_launch_target_setup` would
    newly refuse roughly thirty existing `launch_seat`/`resume_seat` tests that use
    fabricated (never-created) office paths — real scope beyond this wave's own ask. The
    CLI keeps its own protection; an MCP-invoked launch/resume is unchanged.

    THE BOUNDED POST-SPAWN POLL ALSO STAYS CLI-SIDE (a genuine, deliberate divergence found
    during unification, not an oversight): launch_seat's own `can_receive` is a single read
    taken the instant the spawn returns — right for a receipt that must never lie about the
    current instant, wrong for a human at a terminal, whom a fresh claude often has not yet
    self-bound for. Never re-invokes launch_seat (that would risk a second real spawn) —
    only re-polls the SAME already-injected `agents_json` this call already holds, the exact
    8x/1s bound this door has always used."""
    from src.actions.core import Actions
    from src.orchestrator.trigger import _tree_exists, launch_seat

    facts = await _resolve_launch_target(pool, handle, verb="launch")
    if facts is None:
        return 1
    office = facts["anchor_cwd"]
    # THE ONE-SIDED GUARD FAMILY (decision 27259e4d, thread bc11a2d3) — see this function's
    # own docstring for why this stays here rather than in the shared trigger.py shell.
    if not _tree_exists(office):
        print(f"osiris launch: {handle!r} names anchor_cwd={office!r} but it does not "
              "exist on disk — repoint it or create the directory before launch; osiris "
              "never provisions one itself. The anchor is a GRAPH assertion (not a "
              f".osiris pin file) — fix it with: rebind_seat(seat={handle!r}, "
              "new_cwd='<the real directory>') via the osiris MCP tools, or by creating "
              f"{office!r} at that exact path.", file=sys.stderr)
        return 1

    from src.config.settings import get_settings
    st = get_settings()
    out = await launch_seat(
        Actions(pool), caller="operator", target=handle, model=model, settings=st,
        substrate="harness", spawn=spawn, agents_json=agents_json,
        operator_authorized=True)

    status = out.get("status")
    if status == "already-live":
        print(f"already-live: {handle} — a body is already there, nothing started")
        seen_via = out.get("seen_via")
        if seen_via:
            print(f"osiris launch: seen via {', '.join(seen_via)}", file=sys.stderr)
        return 0
    if status != "launched":
        print(f"osiris launch: refused — {out.get('detail', status)}", file=sys.stderr)
        return 1

    dormant = out.get("dormant_history")
    if dormant is not None:
        from src.ingest.sessions import dormant_history_note
        print(f"osiris launch: {handle!r} — {dormant_history_note(dormant)}",
              file=sys.stderr)

    window = out.get("window")
    print(f"osiris launch: spawned {window!r} via claude --bg, requested model="
          f"{out.get('spawned_model') or '(claude CLI default)'}")

    if out.get("can_receive"):
        print(f"  confirmed: find it in `claude agents` as {window!r}")
        return 0

    launch_cwd = facts.get("tree_cwd") or office
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
    print(f"  confirmed: find it in `claude agents` as {window!r}"
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

    # THE SPEND GAP (Thoth dispatch 9378, lane B design's own finding on 9d2aaf4d) — see
    # _cmd_launch_harness's own comment on this same check, above: a separate door, an
    # independent gate, placed after the idempotency check and before the real spawn.
    from src.config.settings import get_settings
    from src.ingest.providers import spend_is_metered
    from src.orchestrator.ceiling import may_spend

    st = get_settings()
    ok, why = await may_spend(pool, cap=st.osiris_daily_usd, metered=spend_is_metered(st))
    if not ok:
        print(f"osiris launch: refused — {why}", file=sys.stderr)
        return 1

    resolved_model = resolve_model(model, facts["intended_model"], wake_default)
    from src.orchestrator.harness_process import claude_pty_argv
    argv = claude_pty_argv(resolved_model)
    from src.orchestrator.trigger import _governed_project_name, _window_name
    name = await _window_name(pool, facts["house"], facts["handle"],
                              await _governed_project_name(
                                  pool, facts["seat_id"], cwd=facts["anchor_cwd"]))
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
    agents_json: AgentsJson | None = None,
) -> int:
    """Bodies a seat: ALWAYS a fresh, persistent `claude --bg` mint (operator ruling
    60c78788 — the launch/resume verb split; see `_cmd_launch_harness`'s own docstring
    for why). `debug=True` (the CLI's `--debug`) keeps the original osiris PTY-broker
    lane alive as an explicit fallback for an incident or a build with no `claude --bg` —
    attachable via `osiris attach`, which the default lane's body is not.
    `pool`/`manager`/`wake_default`/`spawn`/`agents_json` are all injectable (mirrors
    launch_seat's own test seam) — production callers (main()) leave them at their real
    defaults. For continuing a seat's last session instead, see `cmd_resume`."""
    from src.orchestrator.trigger import _claude_agents_json, _spawn_claude_bg
    spawn = spawn or _spawn_claude_bg
    agents_json = agents_json or _claude_agents_json

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
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
                                         agents_json=agents_json)
    finally:
        if owns_pool:
            await pool.close()


# --- resume ------------------------------------------------------------------------------------

async def _cmd_resume_harness(
    handle: str, *, model: str | None, pool: asyncpg.Pool, wake_default: str | None,
    agents_json: AgentsJson, resume_spawn: ResumeSpawn, settings: Settings | None = None,
    clear_stale_record: ClearStaleRecord | None = None,
) -> int:
    """A PERSISTENT `--bg --resume` TURN (Thoth dispatch 6484/6515, superseding the old
    one-shot `-p --resume` lane): same `_lineage_resume_candidate` + `_resume_guard` +
    `resume_spawn` primitives, same order, never a second implementation (#48's lesson).
    Refuses if there is no seat holder, no resumable session, or the gate declines it —
    `osiris resume` never falls through to a fresh mint; that is `osiris launch`'s job,
    a deliberate, separate act (ruling 60c78788: the verb, not a flag, is the property —
    launch always mints fresh, resume always continues; this only changes HOW resume
    continues, never what it means to launch).

    VISIBILITY NOW RIDES `claude agents --json` TOO (the operator's own live complaint —
    "these discrepancies make it impossible to actually resume agents" — fixed at the
    root): decision 536de12f/a829a15d's "a resumed body cannot appear in claude agents"
    was TRUE of the old `-p --resume` one-shot lane and is FALSE of `--bg --resume`,
    confirmed live against the currently installed harness (2.1.258) with a real
    disposable probe session, not inferred from --help text — see `_spawn_claude_bg`'s
    own docstring for the exact verification. A resumed session now genuinely persists:
    it idles after this turn rather than exiting, stays reachable by `claude attach`,
    and is visible to the exact roster the operator was complaining could never see it.

    CLEARS A STALE STOPPED RECORD BEFORE SPAWNING (Thoth dispatch 7543 item 1, mirrors
    `resume_seat`'s own identical fix, trigger.py): `claude rm <sid[:8]>` runs right
    before `resume_spawn`, pre-empting the copy quirk (a leftover harness "stopped"
    record turning `--bg --resume` into a copy) instead of only detecting and adopting
    one after it already happened. The post-spawn NOTE below stays as the safety net."""
    from src.orchestrator.agents import _generation
    from src.orchestrator.seats import seat_receipt
    from src.orchestrator.trigger import (
        _DM_RESUME_PROMPT,
        _adopt_resumed_body,
        _clear_stale_stopped_record,
        _lineage_resume_candidate,
        _resume_guard,
        _resume_office,
    )
    clear_stale_record = clear_stale_record or _clear_stale_stopped_record

    pre = await _resolve_and_guard_launch(
        handle, pool=pool, agents_json=agents_json, verb="resume")
    if isinstance(pre, int):
        return pre
    facts, launch_cwd = pre
    resolved_model = resolve_model(model, facts["intended_model"], wake_default)

    from src.config.settings import get_settings
    st = settings or get_settings()
    holder = ((await seat_receipt(pool, facts["seat_id"])) or {}).get("holder")
    resume_outcome = await _lineage_resume_candidate(
        pool, holder, st, repo=launch_cwd,
        seat_id=facts["seat_id"]) if holder else ["no seat holder on record"]
    resume_log = resume_outcome[1] if isinstance(resume_outcome, tuple) else resume_outcome
    resume = resume_outcome[0] if isinstance(resume_outcome, tuple) else None
    if resume is not None:
        # holder is truthy whenever resume is set — resume_outcome only comes from
        # _lineage_resume_candidate(holder, ...), never the bare-string branch, when
        # holder was falsy. Asserted, not silently narrowed: a violated invariant here
        # should be loud, never a quiet skip of the identity gate.
        assert holder is not None
        # hop count (#173a, mirrored from launch_seat's own identical wiring — ruling
        # 983ec87a, two doors must return the same receipt): READ DIRECTLY off `resume`'s
        # own 6th field now (task #200 residual, decision 6a0b1236/6d6bf4e8) — never
        # re-derived from `len(resume_log) - 1`, which silently miscounts whenever
        # `_lineage_resume_candidate` appends a second log line for the winning hop (e.g.
        # a materialize refusal).
        gate, refusal = await _resume_guard(
            pool, (resume[0], resume[1], resume[2], resume[3]), _generation(holder)[0],
            seat_id=facts["seat_id"], st=st, hop=resume[5], launch_cwd=launch_cwd)
        if gate == "resident-unknown":
            # THE FIX FOR ef88e2bb (operator, 2026-08-17, ruling 7d6815bb) — mirrors
            # launch_seat's own fix exactly (ruling 983ec87a, two doors one receipt): an
            # ABSENCE of signed testimony is not evidence this head belongs to someone
            # else. "crossed-registry" (a POSITIVE finding) still refuses too (below) —
            # osiris resume never mints, whatever the registry finding is; "resident-
            # unknown" gets the SHARPER message naming the exact resume command a human
            # can run to confirm the session themselves.
            print(f"osiris resume: REFUSING — {handle!r} has a possibly-resumable "
                  f"session {resume[0][:8]} but {refusal}. Run `claude -p --resume "
                  f"{resume[0]}` by hand to confirm it yourself; osiris will not resume "
                  "a head it merely couldn't verify.", file=sys.stderr)
            return 1
        if gate is not None:
            resume_log = [*resume_log, f"{gate} guard refused it: {refusal}"]
            resume = None
    if resume is None:
        # NEVER FALLS THROUGH TO FRESH (the whole point of the split, operator ruling
        # 60c78788): `osiris launch` mints fresh; `osiris resume` either resumes or
        # refuses, cleanly, nothing spawned either way.
        print(f"osiris resume: {handle!r} has nothing resumable — "
              f"{_collapse_resume_log(resume_log)} (gate: "
              f"min_tail_bytes={st.osiris_resume_min_tail_bytes}, ceiling="
              f"{st.osiris_resume_ceiling_bytes}b)", file=sys.stderr)
        return 1

    resumed_session_id, materialized_at = resume[0], resume[4]
    # THE SPAWN CWD IS THE OFFICE, ALWAYS — never the tree/launch cwd (operator, 2026-09-03:
    # ~/.osiris is the anchor; the harness resumes the copy in the SPAWN cwd's own slug, so
    # spawning anywhere the canon was not emitted resumes a stale partial). Mirrors
    # trigger.py's dispatch_dm/launch_seat lines exactly (two doors, one receipt).
    spawn_cwd = materialized_at or await _resume_office(
        pool, facts["seat_id"], fallback=facts["anchor_cwd"])
    from src.orchestrator.trigger import _governed_project_name, _window_name
    name = await _window_name(pool, facts["house"], facts["handle"],
                              await _governed_project_name(
                                  pool, facts["seat_id"], cwd=facts["anchor_cwd"]))
    cleared = await clear_stale_record(resumed_session_id[:8])
    await resume_spawn(spawn_cwd, prompt=_DM_RESUME_PROMPT,
                       resume_session=resumed_session_id, name=name, model=resolved_model,
                       allowed_tools=st.osiris_wake_allowed_tools or None)
    # WHAT THE HARNESS ACTUALLY STARTED (2026-09-03): a stopped record still on file makes
    # `--bg --resume` start a COPY under a new id. Read the body back and say so — and
    # adopt the copy as the seat's own continuation before its first act mints a stranger.
    adoption = await _adopt_resumed_body(
        pool, agents_json=agents_json, office=spawn_cwd, requested_sid=resumed_session_id,
        holder=str(holder), project=facts.get("house"))
    if adoption.get("copied"):
        print(f"osiris resume: NOTE — the harness started a COPY (session "
              f"{str(adoption['session_id'])[:8]}) instead of continuing "
              f"{resumed_session_id[:8]}: a stopped background record was still on file "
              f"(`osiris stop` now removes it; `claude rm {resumed_session_id[:8]}` clears "
              f"one by hand). Adopted as {holder}'s own continuation — its ledger and "
              "registry now name this seat's lineage.", file=sys.stderr)
    elif adoption.get("session_id") is None:
        print("osiris resume: NOTE — no body appeared at the office within the check "
              "window; `claude agents --json` is the witness, not this receipt.",
              file=sys.stderr)
    if cleared:
        print(f"osiris resume: cleared a stale stopped record for "
              f"{resumed_session_id[:8]} before spawning — pre-empting the copy quirk, "
              "not just adopting it.", file=sys.stderr)
    print(f"osiris resume: resumed session {resumed_session_id[:8]} at {spawn_cwd} — "
          f"walked {resume[5]} generation(s) back to find it "
          f"({_collapse_resume_log(resume_log)}). Runs persistently under `claude --bg "
          f"--resume`: it idles after this turn rather than exiting, and shows up in "
          f"`claude agents` as {name!r}. To reach it again: send it mail, it wakes on "
          f"the next dispatch; or `claude attach` the session yourself.")
    stamped_model = facts.get("intended_model")
    if stamped_model and resolved_model != stamped_model:
        print(f"  MODEL MISMATCH: spawned on {resolved_model!r} but the seat's own "
              f"stamped intended_model is {stamped_model!r} — never silent (thread "
              "20e4feb6).", file=sys.stderr)
    return 0


async def cmd_resume(
    handle: str, *, model: str | None = None, pool: asyncpg.Pool | None = None,
    wake_default: str | None = None, agents_json: AgentsJson | None = None,
    resume_spawn: ResumeSpawn | None = None, settings: Settings | None = None,
) -> int:
    """Continue a seat's last session PERSISTENTLY via `claude --bg --resume` (Thoth
    dispatch 6484/6515): idles after this turn rather than exiting, visible in `claude
    agents`, reachable again by sending it mail OR by `claude attach`. Never falls
    through to a fresh mint; that is `osiris launch`'s own, separate job (ruling
    60c78788: the verb is the property). `pool`/`agents_json`/`resume_spawn`/`settings`
    are all injectable (mirrors `cmd_launch`'s own test seam) — production callers
    (main()) leave them at their real defaults."""
    from src.orchestrator.trigger import _claude_agents_json, _spawn_claude_bg
    agents_json = agents_json or _claude_agents_json
    resume_spawn = resume_spawn or _spawn_claude_bg

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
                application_name="osiris-cli:resume")
        except Exception as exc:  # noqa: BLE001
            print(f"osiris resume: could not reach postgres at {settings.database_url} — "
                  f"{exc}.", file=sys.stderr)
            return 1
    try:
        return await _cmd_resume_harness(handle, model=model, pool=pool,
                                         wake_default=wake_default, agents_json=agents_json,
                                         resume_spawn=resume_spawn, settings=settings)
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


# --- status (thread 68f1bafa/3703a3a9, the read triangle's own new verb) ---------------------

async def cmd_status(*, as_json: bool = False) -> int:
    url = await _mcp_url()
    return await _call_and_emit_text(
        url, "get_status", {}, as_json=as_json, title="status", error_prefix="osiris status")


# --- search (thread 68f1bafa/3703a3a9, the read triangle's own new verb) ---------------------

async def cmd_search(query: str, *, limit: int = 15, as_json: bool = False) -> int:
    from src import cli_render as render
    from src.orchestrator.mcp_client import call_mcp_tool

    url = await _mcp_url()
    result = await call_mcp_tool(url, "search", {"query": query, "limit": limit})
    if isinstance(result, str):
        print(f"osiris search: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    render.emit(result, as_json=as_json, title=f"search · {query}")
    return 0


# --- fleet -----------------------------------------------------------------------------------

async def cmd_fleet(*, full: bool, as_json: bool = False) -> int:
    from src import cli_render as render
    from src.orchestrator.mcp_client import call_mcp_tool

    url = await _mcp_url()
    if as_json:
        # --json is the machine contract: unchanged, still exactly the server's own
        # response for the caller's own `full`.
        result = await call_mcp_tool(url, "fleet", {"full": full})
        if isinstance(result, str):
            print(f"osiris fleet: {result} — is osiris-mcp running? "
                  "(systemctl --user status osiris-mcp)", file=sys.stderr)
            return 1
        render.emit(result, as_json=True)
        return 0

    # HUMAN MODE (ruling f6b758fc, requirement 3 — SPLIT AFTER A LIVE REGRESSION, Thoth msg
    # 8159): the first cut of this rebuilt the tree CLIENT-SIDE from `fleet()`'s own
    # `registered` rows — but that list is the receipt diet's own capped sample (a handful
    # of rows), never the full node set `render_fleet_tree` needs to fold/group correctly.
    # Live result: 3 sections where the server's own full-data tree carried 36. ONE
    # renderer now: the server's plain `tree` (always computed over the complete node set,
    # `render_fleet_tree`'s only caller shape) is printed as-is, recolored by
    # `paint_fleet_text` — a pure text pass, never a second tree computation. Color depends
    # on THIS terminal (`cli_render.supports_color()`), which the server can never know.
    result = await call_mcp_tool(url, "fleet", {"full": full})
    if isinstance(result, str):
        print(f"osiris fleet: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    tree = result.get("tree")
    if not isinstance(tree, str):
        print("")
        return 0

    paint = render.Paint(render.supports_color())
    print(render.paint_fleet_text(tree, paint))
    return 0


# --- roster ------------------------------------------------------------------------------------

async def _call_and_emit_text(
    url: str, tool: str, params: dict[str, Any], *, as_json: bool, title: str,
    error_prefix: str,
) -> int:
    """Shared body for the read triangle's human-paint commands (thread bad45d61, wave 10:
    backlog/threads/roster/team). `--json` gets the full structured response, unchanged.
    Human mode asks the server for its OWN `render='text'` shape and paints that verbatim
    (cli_render.emit's own `text=` door) — never re-derives grouping from the structured
    rows client-side, the exact fleet-render regression (msg 8160: 3 sections where the
    server tree has 36, from re-deriving off a capped field) this thread names by way of
    the rule it exists to generalize."""
    from src import cli_render as render
    from src.orchestrator.mcp_client import call_mcp_tool

    call_params = dict(params) if as_json else {**params, "render": "text"}
    result = await call_mcp_tool(url, tool, call_params)
    if isinstance(result, str):
        print(f"{error_prefix}: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    if as_json:
        render.emit(result, as_json=True)
        return 0
    text = result.get("text") if isinstance(result, dict) else None
    if text is None:  # an old/odd server shape — fall back to the generic reconstruction
        render.emit(result, as_json=False, title=title)
        return 0
    render.emit(result, as_json=False, title=title, text=text)
    return 0


async def cmd_roster(*, repo: str | None, want_caveats: bool = False, as_json: bool = False) -> int:
    url = await _mcp_url()
    return await _call_and_emit_text(
        url, "roster", {"repo": repo, "want_caveats": want_caveats}, as_json=as_json,
        title=f"roster · {repo}" if repo else "roster", error_prefix="osiris roster")


# --- backlog (thread 68f1bafa/3703a3a9, the read triangle's own new verb) --------------------

async def cmd_backlog(*, all_projects: bool, fleet: bool = False, as_json: bool = False) -> int:
    url = await _mcp_url()
    return await _call_and_emit_text(
        url, "backlog", {"all_projects": all_projects, "fleet": fleet}, as_json=as_json,
        title="backlog · fleet" if fleet else "backlog", error_prefix="osiris backlog")


# --- threads (thread 68f1bafa/3703a3a9, the read triangle's own new verb) --------------------

async def cmd_threads(*, project: str | None, as_json: bool = False) -> int:
    url = await _mcp_url()
    return await _call_and_emit_text(
        url, "threads", {"project": project}, as_json=as_json,
        title=f"threads · {project}" if project else "threads", error_prefix="osiris threads")


# --- team (thread 68f1bafa/3703a3a9, the read triangle's own new verb) -----------------------

async def cmd_team(*, seat: str | None = None, as_json: bool = False,
                   pool: asyncpg.Pool | None = None) -> int:
    """osiris team [--seat <handle>]: a manager's own seats. WITHOUT --seat, calls the MCP
    tool over the wire, self-scoped off the caller's own held seat -- a bare terminal
    session (no mount of its own) gets team()'s own "mount first" refusal, the named gap
    from the read triangle (decision f49d8803). WITH --seat: the gap's own fix (thread
    68f1bafa/642c4754) -- resolves the handle to a seat DIRECTLY against postgres (same
    "resolve then call the shared logic" shape cmd_stop's own operator lane already uses
    for trigger.stop_seat) and calls seats.team_roster, the identical query team() itself
    calls, never a second copy -- WITHOUT changing team()'s own self-scoped MCP contract."""
    from src import cli_render as render

    if seat is None:
        url = await _mcp_url()
        return await _call_and_emit_text(
            url, "team", {}, as_json=as_json, title="team", error_prefix="osiris team")

    from src.orchestrator.seats import seat_by_handle, team_roster

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(settings.database_url, min_size=1, max_size=2,
                                     application_name="osiris-cli:team")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris team: could not reach postgres at {settings.database_url} — "
                  f"{exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        mgr = await seat_by_handle(pool, seat)
        if mgr is None:
            print(f"osiris team: no active seat named {seat!r} (or more than one does)",
                  file=sys.stderr)
            return 1
        rows = await team_roster(pool, mgr["seat_id"], manager_house=mgr["house"])
    finally:
        if owns_pool:
            await pool.close()

    if not rows:
        print(f"osiris team: {mgr['handle']} manages no seats", file=sys.stderr)
        return 1
    # SAME TEXT SHAPE THE MCP DOOR RENDERS (thread bad45d61): render_team_text is the one
    # hand-designed shape for this row set, called from mcp_server.py's own team() — reused
    # here verbatim rather than re-derived, exactly the discipline this thread names.
    from src.orchestrator.textrender import render_team_text

    render.emit({"manager": mgr["handle"], "team": rows}, as_json=as_json,
               title=f"team · {mgr['handle']}",
               text=None if as_json else render_team_text(rows))
    return 0


# --- inbox (thread 68f1bafa/3703a3a9, the read triangle's own new console face; `desk` is
# the operator's own organized queue, this is an ORDINARY project's mailbox) -----------------

async def cmd_inbox(*, project: str, as_json: bool = False) -> int:
    """osiris inbox --project <repo>: a peek at a project's own mailbox, terminal-native.
    Always a peek (never leases) -- settling mail is an agent's own act mid-session, not
    a human glancing from a terminal."""
    url = await _mcp_url()
    return await _call_and_emit_text(
        url, "inbox", {"project": project, "peek": True}, as_json=as_json,
        title=f"inbox · {project}", error_prefix="osiris inbox")


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

GitStatus = Callable[[Path], list[tuple[str, str]]]
RestartServices = Callable[[list[str]], Awaitable[tuple[int, str]]]
InstallUserUnits = Callable[[Path], Awaitable[list[str]]]
UnitStartTimestamps = Callable[[list[str]], Awaitable[dict[str, str]]]


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


def deploy_unit_names(repo_root: Path) -> list[str]:
    """The units `osiris deploy` restarts — DERIVED from `user_unit_sources` (deploy/
    user/*.service), NEVER hand-listed (Thoth's ruling, thread 2a280e07's own follow-up):
    a unit that owns `deploy/user/<name>.service` and never appears in a SEPARATE,
    hand-maintained restart list can silently keep running whatever code was loaded at
    its own last restart — however many `osiris deploy` runs land on main after it. Live
    specimen: `osiris-pulse` was already installed via `deploy/user/osiris-pulse.service`
    (and covered by `install_units`, `_REQUIRED_UNIT_ENV`'s own contract test) but was
    NEVER in the old hand-typed `DEPLOY_UNITS` tuple this function replaces — two full
    days and two separate merged fixes (the reassertion kernel guard, the four
    reasserting sources) ran against its resident, pre-fix process before anyone noticed,
    found only by chasing a live measurement that didn't move. Every unit this repo
    installs is now, structurally, a unit this repo restarts — there is no second list
    to fall out of sync with the first."""
    return [p.stem for p in user_unit_sources(repo_root)]


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


async def _real_unit_start_timestamps(units: list[str]) -> dict[str, str]:
    """`ExecMainStartTimestamp` for each just-restarted unit, straight from systemd — the
    deploy receipt's own PROOF a restart actually replaced the running process (a fresh
    timestamp), not merely that `systemctl restart` exited 0 (which it does even when a
    unit was already stopped, or restarts onto a crash-looping process). Named per unit
    on the receipt so a stale-process gap (osiris-pulse's own two-day specimen, thread
    2a280e07) is visible in the deploy log itself rather than requiring a live
    `systemctl --user show` chase after the fact to notice."""
    out: dict[str, str] = {}
    for unit in units:
        proc = await asyncio.create_subprocess_exec(
            "systemctl", "--user", "show", f"{unit}.service",
            "-p", "ExecMainStartTimestamp", "--value",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        stdout, _ = await proc.communicate()
        ts = stdout.decode(errors="replace").strip()
        if ts:
            out[unit] = ts
    return out


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
        live = c.get("result", {}).get("live_session_repointed")
        if live:
            # THE EXCEPTION'S PRICE (decision 7fe20cc5): this call runs force=True
            # permanently, so a live session's own agent_mounts.project row getting
            # re-pointed is never silent — it lands in deploy output, every time.
            notes.append(f"    NOTE: live session {live} was mounted on the phantom "
                         "side and had its project attribution re-pointed by this "
                         "automatic merge")
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


async def _run_name_alias_automerge(pool: asyncpg.Pool) -> list[str]:
    """#108 PIECE 4 WIRING (Thoth dispatch 6547, addendum to 118a98da) — a third
    post-migration deploy step beside casefold's and remote_url's, same shape and same
    OSIRIS_CASEFOLD_AUTOMERGE default (0 opts out; every other value, including unset,
    executes) — one standing autonomy ruling (22d47acb) over one class of act, never a
    third env var to forget. Every candidate still goes through the SAME fold_project
    door with its own contradiction gate.

    THE RECEIPT NAMES THE STANDING PROCEDURE (item 4, Thoth's own ask): a fold that
    executes here is also the specimen that would have made dsh00001's own
    self-service fold (2026-08-21, ruling 31e5bae1) findable in one query instead of
    the four this lane's own scoping took — so every executed candidate's note points
    at 31e5bae1 directly, and names the alias that grounded it, the same way
    dsh00001's own justification named its reason on the record."""
    from src.actions.core import Actions
    from src.orchestrator.projects import name_alias_duplicate_candidates

    execute = os.environ.get("OSIRIS_CASEFOLD_AUTOMERGE") != "0"
    result = await name_alias_duplicate_candidates(
        Actions(pool), evidence="osiris deploy: automatic name-alias-matched merge "
        "(#108 piece 4, decision 118a98da/31e5bae1) — the survivor's own graph already "
        "asserted this rename as a current name alias before this fold ran",
        actor="osiris-deploy", execute=execute)
    notes = [f"name-alias auto-merge: {'EXECUTED' if execute else 'dry-run'} — "
             f"{len(result['candidates'])} candidate(s), {len(result['skipped'])} skipped"]
    for c in result["candidates"]:
        notes.append(f"  {c['dupe']} -> {c['into']} (alias {c['alias']!r} was already "
                     f"a current name on {c['into']} — see decision 31e5bae1 for the "
                     "standing self-service procedure this fold follows)")
    for s in result["skipped"]:
        notes.append(f"  SKIPPED: {s['survivor']} alias {s['alias']!r} — {s['reason']}")
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
    from src.orchestrator.deploy_guard import (
        _DEPLOY_CURSOR_KEY,
        _git_head,
        resolve_alarms_superseded_by_deploy,
    )
    from src.orchestrator.monitor import set_cursor

    head = _git_head(repo_root)
    if head is not None:
        await set_cursor(pool, _DEPLOY_CURSOR_KEY, head)
        # THE DEPLOY LEG of the boot-watchdog supersession mechanism (operator ruling, DM
        # 7032, following the backlog measurement decision 6354c424): this recorded deploy
        # is itself the ground truth that closes every alarm it now supersedes. Fail-open —
        # the resolver's own internal try/except already guards this, never a deploy failure.
        with contextlib.suppress(Exception):
            await resolve_alarms_superseded_by_deploy(
                pool, repo_root=repo_root, new_head=head, dry_run=False)
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


async def _real_check_false_mint_live(
    pool: asyncpg.Pool, *, agents_json: Any = None, read_exe: Any = None,
    read_cwd: Any = None, read_cmdline: Any = None,
) -> list[dict[str, Any]]:
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
    bucket, never this function (a query has no business writing prose).

    TWO SPECIMENS THAT MUST NEVER BLOCK (Thoth msg 7542 item 3 — five deploys,
    abc056a/89d4605/cc3e4a3/eda0459/f66654a, ran unrecorded over agent:2464d3ad-ii): the
    bg-spare mount-row heartbeat bug (task #204, msg 6997, test_osiris_hook.py's own
    comment) left a false_mint=true generation's `agent_mounts` row refreshing for a
    while after phantom-fold had ALREADY correctly retired it — a heartbeat earned by a
    process that was never the mind it claimed to be. (1) A candidate already carrying
    `retired=true` is a generation the fleet deliberately closed; whatever its stale
    mount row still says, it can never be the halcyon "genuinely live body wrongly
    folded" shape this gate exists to catch, so it is excluded from the query itself.
    (2) Independently — for a candidate that isn't (yet) retired — if EVERY harness body
    `registry_census` matches to it is itself a `claude bg-spare` pre-warm process
    (checked the same way the whisper hook checks itself, `_is_bg_spare_process`'s own
    `/proc/<pid>/cmdline` probe, just server-side against an arbitrary pid instead of
    self), it is excluded too: a spare backing the row is never a genuine occupant, no
    matter how fresh `last_seen` reads."""
    from src.orchestrator.agents import is_occupied_by_a_live_body
    from src.orchestrator.census import _proc_cmdline
    from src.orchestrator.mounts import registry_census

    read_cmdline = read_cmdline or _proc_cmdline
    rows = await pool.fetch(
        "SELECT o.canonical FROM objects o WHERE o.type='Agent' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='false_mint' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  = 'true' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='retired' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "  IS DISTINCT FROM 'true' "
        "AND EXISTS (SELECT 1 FROM agent_mounts m WHERE m.agent_id=o.canonical "
        "  AND m.last_seen > now() - interval '900 seconds') "
        "ORDER BY o.canonical")
    if not rows:
        return []
    census = await registry_census(
        pool, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
    matched = census.get("matched", [])
    out: list[dict[str, Any]] = []
    for r in rows:
        cid = r["canonical"]
        occupied = await is_occupied_by_a_live_body(
            pool, cid, agents_json=agents_json, read_exe=read_exe, read_cwd=read_cwd)
        if occupied:
            pids = [m["pid"] for m in matched
                   if m.get("agent_id") == cid and isinstance(m.get("pid"), int)]
            if pids and all(b"bg-spare" in read_cmdline(p) for p in pids):
                continue  # every body backing this candidate is a spare, not an occupant
        out.append({"agent_id": cid, "harness_confirmed_live": occupied})
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


def _run_install_script(script_rel: str, root: Path) -> str:
    """Run one of this repo's idempotent install-*.sh scripts (task #204, Thoth ruling msg
    6949: "deploy is the one sanctioned hand that writes machine files" — a read-only
    status line that can only ever report STALE was half a mechanism). Every install
    script here (install_gate_hook.sh, install_push_guard_hook.sh, install_commands.sh)
    already follows the SAME idempotent copy-and-compare convention and prints its own
    one-line summary on success — this just runs it and surfaces that line, or a clear
    failure, never raising past this wrapper (a broken installer must not crash the rest
    of deploy's own report)."""
    import subprocess

    script = root / script_rel
    if not script.is_file():
        return f"{script_rel}: SOURCE MISSING — nothing installed"
    try:
        result = subprocess.run(
            ["sh", str(script)], cwd=root, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"{script_rel}: could not run ({exc})"
    if result.returncode != 0:
        return (f"{script_rel}: FAILED (exit {result.returncode}) — "
                f"{result.stderr.strip() or result.stdout.strip()}")
    return result.stdout.strip() or f"{script_rel}: ran, no output"


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
    unit_start_timestamps: UnitStartTimestamps = _real_unit_start_timestamps,
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
    Also prints (never gates) a NOTE when any seat carries a current `anchor_cwd` outside
    the office root alongside the correct one (ruling 23771416) — this population was
    found by an operator hitting a broken resume, and should never be found that way again.

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
        from src.orchestrator.mailbox import send_message
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
        for note in await _run_name_alias_automerge(pool):
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

        # THE DISCONNECT WARNING (Thoth's plan, thread d96167c6, 2026-09-05): a live
        # specimen (2026-09-01 19:10:57) restarted osiris-mcp with 15 agents live —
        # every streamable-HTTP session died silently, 22 requests came back 404, and the
        # OPERATOR's first symptom was "graph unreachable." health/smoke probe a FRESH
        # connection and can never see a stale one, so the deploy ledger read clean while
        # every live session was in fact broken. Reconnect itself is the harness client's
        # own job (automount re-adopts on the next mount()) — the warning is the whole
        # fix. A mail hiccup here must never block or fail the deploy itself.
        deploy_sha = _git_head(root)
        try:
            await send_message(
                pool, from_agent="deploy:disconnect-warning", from_project="osiris",
                to_project="osiris", grade="fyi",
                body=f"deploy {deploy_sha[:8] if deploy_sha else '?'} restarting "
                     "osiris-mcp — your next call reconnects (streamable-HTTP sessions "
                     "do not survive the restart; automount re-adopts on your next "
                     "mount()).")
        except Exception as exc:  # noqa: BLE001
            print(f"NOTE: pre-restart disconnect warning failed to send: {exc}")

        units = deploy_unit_names(root)
        rc, out = await restart(units)
        if rc != 0:
            print(f"osiris deploy: restart failed (exit {rc}): {out}", file=sys.stderr)
            return 1
        print(f"osiris deploy: restarted {', '.join(units)}")
        # THE RECEIPT'S OWN PROOF (thread 2a280e07 follow-up): a per-unit start timestamp,
        # not just an exit-0 from `systemctl restart` — osiris-pulse's own two-day-stale
        # process is exactly the gap a receipt with no timestamp per unit couldn't have
        # caught even after the fact.
        for unit, ts in (await unit_start_timestamps(units)).items():
            print(f"  {unit}: started {ts}")

        health_ready, health_waited = await wait_for_health()
        if health_ready:
            print(f"health: up after {health_waited:.0f}s" if health_waited
                  else "health: up immediately")
            try:
                await send_message(
                    pool, from_agent="deploy:disconnect-warning", from_project="osiris",
                    to_project="osiris", grade="fyi",
                    body=f"deploy {deploy_sha[:8] if deploy_sha else '?'}: osiris-mcp is "
                         "back up — reconnect now.")
            except Exception as exc:  # noqa: BLE001
                print(f"NOTE: post-restart reconnect notice failed to send: {exc}")
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

        # THE ANCHOR INVARIANT (ruling 23771416, msg 6546/6577) — INFORMATIONAL ONLY,
        # never gating: unlike the halcyon gate above (an architectural-safety refusal),
        # a seat's own stray anchor_cwd is an identity-hygiene defect with no deploy-time
        # blast radius, so this only surfaces what the standalone detector would show,
        # armed here so the population is discovered by routine deploy traffic rather than
        # by an operator hitting a broken `osiris resume` again (the root cause of THIS
        # session's own specimens). `heal-seat-anchor` is the repair door, named in the note.
        with contextlib.suppress(Exception):  # an advisory note must never crash a deploy
            from src.actions.core import Actions
            from src.orchestrator.identity_heal import detect_anchor_invariant_violations

            anchor_findings = await detect_anchor_invariant_violations(Actions(pool))
            both_axes = ({m["seat"] for m in anchor_findings["multi_current"]}
                        & {r["seat"] for r in anchor_findings["outside_root"]})
            if both_axes:
                print(f"NOTE: anchor invariant — {len(both_axes)} seat(s) carry a current "
                      f"anchor_cwd outside the office root ALONGSIDE the correct one: "
                      f"{sorted(both_axes)}. `osiris heal-seat-anchor <handle> --because "
                      "... [--apply]` repairs one seat at a time; never auto-run here.")

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

        print(_run_install_script("scripts/install_push_guard_hook.sh", root))
        from scripts.push_guard import hook_status
        print(hook_status(root))

        print(_run_install_script("scripts/install_gate_hook.sh", root))
        from scripts.gate_hook import hook_status as gate_hook_status
        print(gate_hook_status(root))

        print(_run_install_script("scripts/install_commands.sh", root))
        from scripts.commands_status import commands_status
        print(commands_status(root))

        print(_run_install_script("scripts/install_prune_timers.sh", root))

        return 1 if fails else 0
    finally:
        if owns_pool:
            await pool.close()


# --- merge / unmerge ---------------------------------------------------------------------------

async def cmd_merge(
    dupe: str, into: str, evidence: str, *, actor: str, force: bool = False,
    because: str = "", pool: asyncpg.Pool | None = None,
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
    merges only, matching the MCP tool's own conditional exactly — is queried here too.

    `--force` (+ `--because`): decision 7fe20cc5's liveness guard override, SoftwareProject
    folds only — self stays open by default, a different lineage's live target refuses
    unless forced."""
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
        out = await _merge(Actions(pool), dupe=dupe, into=into, evidence=evidence,
                           actor=actor, force=force, because=because or None)
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
    dupe: str, into: str, evidence: str, *, actor: str, force: bool = False,
    because: str = "", pool: asyncpg.Pool | None = None,
) -> int:
    """DEPRECATED ALIAS (dispatch 3683): fold_project no longer exists as an MCP tool —
    it collapsed into merge() (ruling 31c02dca, decision a926a8d0) and the CLI never
    followed, the exact "two halves of this house use different words for one act"
    specimen the operator's own consistency ask named. Kept working, hidden from the
    front-door listing, forwarding straight to cmd_merge with the identical arguments —
    never break a human's muscle memory silently, but never advertise the old name either."""
    print("osiris fold-project is deprecated — use `osiris merge` (identical arguments, "
          "same evidence-gated fold). Continuing as merge.", file=sys.stderr)
    return await cmd_merge(dupe, into, evidence, actor=actor, force=force, because=because,
                           pool=pool)


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
    from src import cli_render as render

    # THE --json PROMISE HELD ON THE REFUSAL PATH TOO (Thoth dispatch 6746, specimen B):
    # this used to short-circuit with a bare stderr print BEFORE reaching render.emit
    # below, so `--json` was silently ignored on exactly the path a script most needs
    # it. Same shape as cmd_show's own error handling — emit unconditionally, exit code
    # carries the verdict.
    render.emit(out, as_json=as_json, title="unmerge")
    return 1 if "error" in out else 0


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
    ruling: str | None = None, pool: asyncpg.Pool | None = None,
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
        out = await charter_for(Actions(pool), seat_id, repos, because=because, actor=actor,
                                ruling=ruling)
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


# --- settings (THE SETTINGS MENU, thread f4498ab304e4 piece 1) ---------------------------------

async def cmd_settings(
    action: str, *, key: str | None = None, value: str | None = None,
    because: str = "", ruling: str | None = None, scope_id: str = "",
    actor: str = _CONSOLE_ACTOR, as_json: bool = False, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris settings <list|get|set> ... — the console-script door onto
    settings_service.{list_settings,get_setting,write_setting}, the SAME functions the
    `settings` MCP tool wraps (no duplicated logic — the CLI backup_settings itself
    never got, per Thoth's own ask on thread f4498ab304e4). `value` is a JSON string
    for anything beyond a bare number/string (e.g. `'true'`, `'{"a":1}'`, `'["x","y"]'`)
    — parsed the same way `osiris proposal`'s own `--candidate` flag already is."""
    import json as _json

    from src.orchestrator import settings_service

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings

        apply_dev_fallback()
        settings = get_settings()
        from src.db.pool import create_pool
        try:
            pool = await create_pool(
                settings.database_url, min_size=1, max_size=2,
                application_name="osiris-cli:settings")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris settings: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        if action == "list":
            out: dict[str, Any] = {"settings": await settings_service.list_settings(pool)}
        elif action == "get":
            if not key:
                print("osiris settings get: a key is required", file=sys.stderr)
                return 1
            out = await settings_service.get_setting(pool, key)
        elif action == "set":
            if not key:
                print("osiris settings set: a key is required", file=sys.stderr)
                return 1
            parsed_value: Any = None
            if value is not None:
                try:
                    parsed_value = _json.loads(value)
                except _json.JSONDecodeError:
                    parsed_value = value  # a bare unquoted string ('claude-fable-5') is legal
            out = await settings_service.write_setting(
                pool, key, parsed_value, actor=actor, because=because, scope_id=scope_id,
                ruling=ruling)
        else:
            print(f"osiris settings: action must be 'list', 'get', or 'set' (got {action!r})",
                  file=sys.stderr)
            return 1
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris settings {action}: refused — {out['error']}", file=sys.stderr)
        return 1
    from src import cli_render as render
    render.emit(out, as_json=as_json, title=f"settings {action}")
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
    and None-check are duplicated here on purpose, not softened. Also mirrors the MCP
    receipt's own `practice` row (thread 55e5ac72, msg 9123, the SAME `practices` Function
    both doors read through) — printed after the confirmation line, so this door's write
    is never invisible on its own receipt either."""
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
    row: dict[str, object] | None = None
    try:
        try:
            pid = await amend_practice(Actions(pool), ref, amendment, source=actor)
        except ValueError as e:
            print(f"osiris amend-practice: refused — {e}", file=sys.stderr)
            return 1
        if pid is not None:
            from src.orchestrator import compositions as comp
            rows = await comp._fn_practices(pool, None, {"id": str(pid)})
            row = rows[0] if rows else None
    finally:
        if owns_pool:
            await pool.close()
    if pid is None:
        print(f"osiris amend-practice: refused — no practice matches {ref!r}",
              file=sys.stderr)
        return 1
    print(f"amended {pid}: {amendment.strip()}")
    if row is not None:
        print(f"practice now reads: {row['statement']} (confirmed={row['confirmed']})")
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


# --- send / decide / thread (THE WRITE TRIANGLE, dispatch a354ba28, msg 7882 item 2) ------------
#
# THE READ TRIANGLE'S OWN COUNTERPART (commit 841fad0 built get_status(render='text') +
# commands/status.md as "wave 1" of reading the fleet from a bare terminal); these three
# close the matching gap on the WRITE side — mail, a decision, and closing a thread were
# each reachable only through an MCP client before this, so an operator (or a script) at a
# plain shell had no way to post to the fleet's own memory without one. Each calls the SAME
# orchestrator function its MCP twin wraps (mailbox.send_message / capture.record_decision /
# capture.resolve_thread(_bulk)), same "no duplicated guard" law annotate-thread/amend-
# decision above already keep. Named for Khnum's parity gate (5bf6447c): each command's own
# param set matches its MCP counterpart's exactly (tests/test_cli_mcp_parity.py), via
# CLI_TO_MCP_NAME for `decide`->`record_decision` and `thread`->`thread:resolve`; `send`
# needs no override, its name already matches.

async def cmd_send(
    body: str, *, to: str | None = None, to_agent: str | None = None,
    reply_to: int | None = None, desk: str | None = None, grade: str | None = None,
    require_seat: bool = False, threads: list[str] | None = None,
    want_prior_art: bool = False, want_listener: bool = False,
    from_project: str | None = None, actor: str = _CONSOLE_ACTOR,
    as_json: bool = False, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris send <body> [--to PROJECT | --to-agent AGENT] ... — the console-script door
    onto mailbox.send_message, the SAME function the send MCP tool wraps (no duplicated
    guard: `to`'s unknown-project refusal, the send-door addressing guard on a mismatched
    room, and every seat-resolution refusal below are exactly send_message's own).

    `--from-project` is CLI-ONLY (CLI_ONLY_PARAMS, test_cli_mcp_parity.py): the MCP tool
    derives it from the caller's own mount (`ident.project`) — a bare console caller has
    no mount to derive it from, so this names the gap with an explicit flag instead of
    guessing or leaving broadcasts from a console unrouteable.

    A NAMED RESIDUAL (not every MCP-side receipt field is reproduced): `dispatch` (the
    immediate wake/poke leg) and `listener` (when `--want-listener`) ARE included, same
    as the MCP receipt; the "crossed-mail" peer-thread warning and the ephemeral-spawn
    warning are not — a real, bounded gap, not a silent omission, left for whoever next
    finds a console caller actually needs them."""
    from src.orchestrator.mailbox import send_message

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(settings.database_url, min_size=1, max_size=4,
                                     application_name="osiris-cli:send")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris send: could not reach postgres at {settings.database_url} — "
                  f"{exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        prior: list[dict[str, Any]] = []
        if want_prior_art and (grade == "ask" or to_agent):
            from src.mcp_server import _surface_prior_art
            prior = await _surface_prior_art(pool, body, repo=from_project, actor=actor)
        try:
            res = await send_message(
                pool, from_agent=actor, from_project=from_project, to_project=to,
                to_agent=to_agent, body=body, reply_to=reply_to, desk_kind=desk,
                grade=grade, require_seat=require_seat, threads=threads)
        except ValueError as e:
            print(f"osiris send: refused — {e}", file=sys.stderr)
            return 1
        out: dict[str, Any] = {"sent": res["id"], "from": actor}
        if res["thread_id"] is not None:
            out["thread"] = res["thread_id"]
        if res.get("dedup"):
            out["dedup"] = "identical recent message already queued — not re-posted"
        if res.get("threads_stamped"):
            out["threads_stamped"] = res["threads_stamped"]
        if res.get("addressee_resolved"):
            out["addressee_resolved"] = res["addressee_resolved"]
        if want_prior_art and prior:
            out["prior_art"] = [{"id": p["id"], "type": p.get("type"),
                                 "summary": p.get("summary", "")} for p in prior]
        if res["to_agent"]:
            out["dm_to"] = res["to_agent"]
            out["seat"] = res.get("seat")
            out["lineage_head"] = res.get("lineage_head")
            if want_listener:
                from src.orchestrator import mounts
                out["listener"] = await mounts.agent_liveness(
                    pool, res.get("lineage_head") or res["to_agent"])
            if res.get("redirect"):
                out["redirect"] = res["redirect"]
            if not res["dedup"]:
                try:
                    from src.orchestrator.trigger import dispatch_dm
                    out["dispatch"] = await dispatch_dm(
                        pool, addressee=res["to_agent"], msg_id=res["id"], sender=actor)
                except Exception as exc:  # noqa: BLE001 - the send already committed; confess
                    out["dispatch"] = {"mode": "deferred",
                                       "detail": f"immediate dispatch failed ({exc}) — "
                                                "the worker sweep is the backstop"}
        else:
            from src.orchestrator import mounts
            dest = res["to"]
            out["to"] = dest
            if want_listener:
                from datetime import UTC, datetime, timedelta

                last_seen = await mounts.project_last_seen(pool, dest)
                out["listener"] = {
                    "live": bool(last_seen and datetime.now(UTC) - datetime.fromisoformat(
                        last_seen) < timedelta(minutes=15)),
                    "last_seen": last_seen}
            if not res["dedup"]:
                try:
                    from src.orchestrator.trigger import dispatch_broadcast
                    out["dispatch"] = await dispatch_broadcast(
                        pool, project=dest, msg_id=res["id"], sender=actor)
                except Exception as exc:  # noqa: BLE001 - the send already committed; confess
                    out["dispatch"] = {"mode": "deferred",
                                       "detail": f"immediate dispatch failed ({exc}) — "
                                                "the worker sweep is the backstop"}
            from src.config.settings import get_settings
            from src.orchestrator.mailbox import project_deliverable_count
            out["backlog"] = await project_deliverable_count(
                pool, dest, lease_secs=get_settings().osiris_mail_lease_secs)
    finally:
        if owns_pool:
            await pool.close()
    from src import cli_render as render
    render.emit(out, as_json=as_json, title="send")
    return 0


async def cmd_decide(
    summary: str, *, kind: str = "ruling", rationale: str | None = None,
    repo: str | None = None, grounds: list[str] | None = None,
    protocol: str | None = None, supersedes: str | None = None,
    resolves: list[str] | None = None, obsoletes: list[str] | None = None,
    confirms: list[str] | None = None, refutes: str | None = None,
    implements: str | None = None, rediscovers: list[str] | None = None,
    bears_on: list[str] | None = None, narrows: list[str] | None = None,
    cites: list[str] | None = None, ack_prior_art: bool = False,
    unlinked_because: str | None = None, operator_authorized: bool = False,
    actor: str = _CONSOLE_ACTOR,
    as_json: bool = False, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris decide <summary> [--kind K] [--rationale R] ... — the console-script door
    onto capture.record_decision, the SAME function the record_decision MCP tool wraps
    (no duplicated guard: idempotent-retry-reuses-the-same-decision, the declare-or-
    refuse link-kind gate, and `supersedes`/`resolves`'s own free-text resolution
    — `_find_decision`/`_find_thread` — are exactly record_decision's own, untouched
    here; those two are the only link params it resolves internally).

    A NAMED, BOUNDED GAP for the rest: `grounds`/`confirms`/`rediscovers`/`bears_on`/
    `narrows`/`cites`/`implements`/`refutes` are typed as PRE-RESOLVED UUIDs by
    record_decision itself (the MCP wrapper does the free-text/short-id resolution
    BEFORE calling it, via the same `_find_decision`/`_find_thread`/`_find_practice`
    helpers) — reproducing that whole resolution ladder here would be the exact
    duplicated-guard risk this door's own law forbids, so this console door accepts
    only an EXACT uuid for each (never a canonical string or short-id prefix), and
    `--ack-prior-art` is accepted for CLI/MCP name parity but has no effect — no
    prior-art search runs from a bare terminal, so there is nothing to acknowledge.

    `--operator-authorized` (decision 12efe065): this decision carries the operator's
    OWN authority — mints a `ruled_by` edge to the operator's own Person object.
    A bare terminal is exactly where a human operator IS typing directly, so this flag
    is real here, not a stub — set it only when this decision really is the operator's
    ruling.

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT, same rule amend-decision/
    annotate-thread above keep — but this hand-builds a LEANER receipt than the MCP
    wrapper's own (no `content_landed`/`prior_art`/`resolved_thread` echo): a named,
    bounded gap, not a silent one."""
    import uuid as uuid_mod

    from src.actions.core import Actions
    from src.orchestrator.capture import record_decision

    def _uuids(vals: list[str] | None, flag: str) -> list[uuid_mod.UUID] | None:
        if not vals:
            return None
        try:
            return [uuid_mod.UUID(v) for v in vals]
        except ValueError as e:
            raise SystemExit(
                f"osiris decide: {flag} takes an exact uuid only (no short-id/prose "
                f"resolution from the console) — {e}") from e

    def _uuid1(val: str | None, flag: str) -> uuid_mod.UUID | None:
        if val is None:
            return None
        try:
            return uuid_mod.UUID(val)
        except ValueError as e:
            raise SystemExit(
                f"osiris decide: {flag} takes an exact uuid only (no short-id/prose "
                f"resolution from the console) — {e}") from e

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(settings.database_url, min_size=1, max_size=4,
                                     application_name="osiris-cli:decide")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris decide: could not reach postgres at {settings.database_url} — "
                  f"{exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        try:
            did = await record_decision(
                Actions(pool), summary, kind=kind, rationale=rationale, repo=repo,
                source=actor, grounds=_uuids(grounds, "--grounds"), protocol=protocol,
                supersedes=supersedes, resolves=resolves, obsoletes=obsoletes,
                confirms=_uuids(confirms, "--confirms"),
                implements=_uuid1(implements, "--implements"),
                rediscovers=_uuids(rediscovers, "--rediscovers"),
                bears_on=_uuids(bears_on, "--bears-on"),
                narrows=_uuids(narrows, "--narrows"), cites=_uuids(cites, "--cites"),
                refute_id=_uuid1(refutes, "--refutes"),
                unlinked_because=unlinked_because,
                operator_authorized=operator_authorized)
        except ValueError as e:
            print(f"osiris decide: refused — {e}", file=sys.stderr)
            return 1
    finally:
        if owns_pool:
            await pool.close()
    out = {"id": str(did), "kind": kind, "summary": summary.strip()}
    if ack_prior_art:
        out["note"] = "--ack-prior-art has no effect from the console — no prior-art " \
                       "search runs here (see this command's own docstring)"
    from src import cli_render as render
    render.emit(out, as_json=as_json, title="decide")
    return 0


async def cmd_thread(
    ref: list[str], *, because: str | None = None, artifact: str | None = None,
    dry_run: bool = True, actor: str = _CONSOLE_ACTOR, as_json: bool = False,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris thread <ref>... [--because W] [--artifact A] [--dry-run/--no-dry-run] —
    the console-script door onto the `thread` MCP tool's own `action='resolve'` branch
    (the terminal-native reading of a bare "thread" verb: closing one). A DELIBERATE
    NARROWING (same shape as `desk`/`show`'s own declared narrowings, NO_MCP_EQUIVALENT's
    reasoning in test_cli_mcp_parity.py): `thread`'s other three actions (annotate/
    correct_summary/reclassify) have no console door here — annotate already has its own
    (`annotate-thread`); the other two are a real, left-open gap, not silently dropped.

    ONE ref resolves through capture.resolve_thread directly (no dry_run — the single-ref
    primitive has never had one); MORE THAN ONE routes through resolve_threads_bulk,
    where --dry-run (default True, matching the MCP tool's own default) actually applies
    — this mirrors `_thread_action_impl`'s own resolve branch exactly, not a
    reimplementation of it."""
    from src.actions.core import Actions
    from src.orchestrator.capture import resolve_thread, resolve_threads_bulk

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(settings.database_url, min_size=1, max_size=4,
                                     application_name="osiris-cli:thread")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris thread: could not reach postgres at {settings.database_url} — "
                  f"{exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        if len(ref) == 1:
            tid = await resolve_thread(
                Actions(pool), ref[0], because=because, artifact=artifact, source=actor)
            if tid is None:
                print(f"osiris thread: refused — no thread matches {ref[0]!r}",
                      file=sys.stderr)
                return 1
            out: dict[str, Any] = {"id": str(tid), "status": "resolved"}
        else:
            out = await resolve_threads_bulk(
                Actions(pool), ref, because=because or "", artifact=artifact,
                dry_run=dry_run, source=actor)
    finally:
        if owns_pool:
            await pool.close()
    from src import cli_render as render
    render.emit(out, as_json=as_json, title="thread")
    return 0


# --- proposal (miners as last resort, item 2, decision ac892cd9) -------------------------------

async def cmd_proposal(
    action: str, *, from_id: str | None = None, link_type: str | None = None,
    candidate: str | None = None, confidence: float | None = None,
    owner: str | None = None, miner: str | None = None,
    proposal_ref: str | None = None, reason: str | None = None,
    actor: str = _CONSOLE_ACTOR, as_json: bool = False,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris proposal <propose|accept|reject> ... — the console-script door onto
    `_proposal_action_impl`, the SAME function the `proposal` MCP tool wraps (miners as
    last resort, decision ac892cd9). `--candidate` takes a JSON string
    ('{"kind":"link",...}' or '{"kind":"object",...}') since a graph write's own shape
    has no flat flag equivalent."""
    import json as _json

    from src.mcp_server import _proposal_action_impl

    parsed_candidate = None
    if candidate is not None:
        try:
            parsed_candidate = _json.loads(candidate)
        except _json.JSONDecodeError as exc:
            print(f"osiris proposal: --candidate is not valid JSON — {exc}",
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
            pool = await create_pool(settings.database_url, min_size=1, max_size=2,
                                     application_name="osiris-cli:proposal")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris proposal: could not reach postgres at {settings.database_url} "
                  f"— {exc}. Set DATABASE_URL, or start the dev instance.", file=sys.stderr)
            return 1
    try:
        out = await _proposal_action_impl(
            pool, actor, action, from_id=from_id, link_type=link_type,
            candidate=parsed_candidate, confidence=confidence, owner=owner, miner=miner,
            proposal_ref=proposal_ref, reason=reason)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris proposal: refused — {out['error']}", file=sys.stderr)
        return 1
    from src import cli_render as render
    render.emit(out, as_json=as_json, title="proposal")
    return 0


# --- rebind-seat / correct-pin-value (thread 6437, #199's parity lane) --------------------------
#
# THE JESUS/CHAD PATH, FROM A TERMINAL: a seat self-reconciling ran exactly
# merge(dupe,into) -> rebind_seat(seat,new_cwd) -> correct_pin_value(key,value) through MCP
# (msg 6374, thread 6369). merge/unmerge already had a console door; these two did not, so a
# human doing the SAME reconciliation by hand — the whole point of a CLI, per the operator's
# own "cli shared, mcp agent, slash for human" model — had no way to run it. Both below call
# the SAME orchestrator function their MCP twin wraps (mounts.rebind_seat /
# offices.correct_own_pin_value), no parallel implementation — #135's parity gate ruling
# binds here exactly as it did for `osiris bootstrap`.


async def cmd_rebind_seat(
    seat_or_agent: str, new_cwd: str, *, actor: str, extract: bool = False,
    because: str = "", force: bool = False, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris rebind-seat <seat> <new_cwd> --actor <who> — the console-script door
    onto mounts.rebind_seat, the SAME function the rebind_seat MCP tool wraps (no duplicated
    resolution: the claimed-name / raw-agent-id / unclaimed-seat-handle fallback chain, and
    the extract=True seat-offices-move shape, are exactly rebind_seat's own, untouched here).

    TWO DOORS ONTO ONE FUNCTION MUST RETURN THE SAME RECEIPT: prints the full result dict
    the MCP tool would also return, nothing dropped — the same discipline charter-for's own
    CLI door established (thread 2474)."""
    from src.actions.core import Actions
    from src.orchestrator.mounts import rebind_seat

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
                application_name="osiris-cli:rebind-seat")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris rebind-seat: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await rebind_seat(Actions(pool), seat_or_agent=seat_or_agent, new_cwd=new_cwd,
                                actor=actor, extract=extract, because=because, force=force)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris rebind-seat: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"rebound {seat_or_agent} -> {new_cwd}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_correct_pin_value(
    handle_or_agent: str, key: str, value: str, reason: str, *,
    pool: asyncpg.Pool | None = None, office_root: Path | None = None,
) -> int:
    """osiris correct-pin-value <seat> <key> <value> --because <reason> --actor <who> — the
    console-script door onto offices.correct_own_pin_value, the SAME function the
    correct_pin_value MCP tool wraps. THE ONE DIFFERENCE FROM ITS MCP TWIN, NAMED HONESTLY:
    the MCP tool is self-scoped by construction (`ident.agent_id`, the mounted caller — it can
    only ever correct ITS OWN seat's pin). A terminal has no mounted identity to be self about,
    so this door takes an EXPLICIT target instead — same shape rebind-seat's own console door
    already uses (`seat_or_agent`, resolved the identical way: resolve_handle, falling back to
    a raw agent id that genuinely exists). correct_own_pin_value itself is untouched — its own
    held_seat resolution, its own refusal on a caller holding no seat, its own required-reason
    and existing-key-only guards all still apply, now just to a NAMED seat rather than an
    implicit one."""
    from src.actions.core import Actions
    from src.orchestrator.agents import resolve_handle
    from src.orchestrator.offices import correct_own_pin_value

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
                application_name="osiris-cli:correct-pin-value")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris correct-pin-value: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        actions = Actions(pool)
        agent_id = await resolve_handle(actions, handle_or_agent)
        if agent_id is None:
            exists = await pool.fetchval(
                "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
                handle_or_agent)
            agent_id = handle_or_agent if exists else None
        if agent_id is None:
            print(f"osiris correct-pin-value: refused — no such claimed seat or live agent: "
                  f"{handle_or_agent!r}", file=sys.stderr)
            return 1
        out = await correct_own_pin_value(pool, agent_id, key, value, reason=reason,
                                          office_root=office_root)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris correct-pin-value: refused — {out['error']}", file=sys.stderr)
        return 1
    if not out.get("written"):
        print(f"office: already {value!r} — nothing written (old_value={out.get('old_value')!r})")
    else:
        print(f"corrected {out.get('seat_id', handle_or_agent)}'s {key}: "
              f"{out['old_value']!r} -> {out['new_value']!r}")
        print(f"  path: {out['path']}  backup: {out['backup']}")
    # THE OFFICE-ONLY EARLY RETURN WAS A REAL BUG (found live, Jesus's own workspace pin,
    # 2026-09-04): correct_own_pin_value ALWAYS checks/corrects the anchor and workspace
    # copies too (ruling b30e2b38/thread 6483-6504), regardless of whether the office copy
    # itself needed a write -- a caller whose office was already correct but whose second or
    # third copy genuinely got corrected used to see "already X -- nothing written" with no
    # mention the write happened. Report each present copy separately, every time.
    for copy in ("anchor", "workspace"):
        detail = out.get(copy)
        if detail is None:
            continue
        if detail.get("error"):
            print(f"{copy}: refused — {detail['error']}")
        elif detail.get("corrected"):
            print(f"{copy}: corrected — path: {detail['path']}")
        else:
            print(f"{copy}: already {value!r} — nothing written "
                  f"(old_value={detail.get('old_value')!r})")
    return 0


async def cmd_transition_seat_project(
    handle_or_agent: str, *, because: str = "", fabricated_project: str | None = None,
    real_project: str | None = None, repos: list[str] | None = None, apply: bool = False,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris transition-seat-project <seat> [--fabricated-project P] [--real-project P]
    [--repos R ...] [--because <reason>] [--apply] — the console-script door onto
    transition.transition_seat_project, the SAME function the transition_seat_project
    MCP tool wraps. THE ONE DIFFERENCE FROM ITS MCP TWIN, NAMED HONESTLY: the MCP tool
    is self-scoped by construction (the mounted caller's own agent_id) — a terminal has
    no mounted identity to be self about, so this door takes an EXPLICIT target, same
    resolution shape correct-pin-value's own console door already uses.

    `--apply` is required to actually write — dry_run=True is the default, matching
    every other repair verb in this house."""
    from src.orchestrator.agents import resolve_handle
    from src.orchestrator.transition import transition_seat_project

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
                application_name="osiris-cli:transition-seat-project")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris transition-seat-project: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        from src.actions.core import Actions
        agent_id = await resolve_handle(Actions(pool), handle_or_agent)
        if agent_id is None:
            exists = await pool.fetchval(
                "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
                handle_or_agent)
            agent_id = handle_or_agent if exists else None
        if agent_id is None:
            print(f"osiris transition-seat-project: refused — no such claimed seat or "
                  f"live agent: {handle_or_agent!r}", file=sys.stderr)
            return 1
        out = await transition_seat_project(
            pool, agent_id, fabricated_project=fabricated_project,
            real_project=real_project, because=because, repos=repos, dry_run=not apply)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris transition-seat-project: refused — {out['error']}", file=sys.stderr)
        return 1
    if out.get("dry_run"):
        print(f"PLAN for {out['seat']} — {out['fabricated_project']} -> "
              f"{out['real_project']} (dry run, pass --apply to execute):")
        for step, detail in out["plan"].items():
            print(f"  {step}: {detail if detail is not None else 'already correct — no-op'}")
        return 0
    print(f"transitioned {out['seat']} — {out['fabricated_project']} -> "
          f"{out['real_project']}")
    for step, detail in out.get("steps", {}).items():
        print(f"  {step}: {detail}")
    return 0


async def cmd_heal_seat_anchor(
    seat_or_handle: str, *, because: str, apply: bool = False, actor: str,
    pool: asyncpg.Pool | None = None, office_root: Path | None = None,
) -> int:
    """osiris heal-seat-anchor <seat> --because <reason> [--apply] — the console-script
    door onto identity_heal.heal_seat_anchor_third_party, the SAME function the
    heal_seat_anchor_third_party MCP tool wraps. Always the third-party door, same
    reasoning as correct-pin-value's own console twin: a terminal has no mounted identity
    to be self-scoped about, so this always names an EXPLICIT target and always requires
    `--because` — THE ANCHOR INVARIANT (ruling 23771416): a seat's anchor_cwd is identity,
    always `<office_root>/<handle>`, never wherever a session happened to be sitting.

    `seat_or_handle` accepts either — a bare `seat:...` canonical passes straight through;
    anything else resolves via `seats.seats_by_handle` (case-insensitive, house-agnostic,
    the same lookup `osiris launch`'s own target resolution uses), refusing loudly on zero
    or ambiguous (>1) matches rather than guessing.

    `--apply` is required to actually write — dry_run=True is the default, matching every
    other repair verb in this house (the backfill scripts' own `--apply` convention)."""
    from src.actions.core import Actions
    from src.orchestrator.identity_heal import heal_seat_anchor_third_party
    from src.orchestrator.seats import seats_by_handle

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
                application_name="osiris-cli:heal-seat-anchor")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris heal-seat-anchor: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        if seat_or_handle.startswith("seat:"):
            seat_id = seat_or_handle
        else:
            matches = await seats_by_handle(pool, seat_or_handle)
            if not matches:
                print(f"osiris heal-seat-anchor: refused — no active seat holds handle "
                      f"{seat_or_handle!r}", file=sys.stderr)
                return 1
            if len(matches) > 1:
                print(f"osiris heal-seat-anchor: refused — {seat_or_handle!r} is "
                      f"ambiguous, {len(matches)} seats share it: {matches}. Use the "
                      "seat's own canonical id instead.", file=sys.stderr)
                return 1
            seat_id = matches[0]
        out = await heal_seat_anchor_third_party(
            Actions(pool), seat_id=seat_id, because=because, actor=actor,
            dry_run=not apply, office_root=office_root)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris heal-seat-anchor: refused — {out['error']}", file=sys.stderr)
        return 1
    if out.get("healed") is False:
        print(f"{seat_id}: {out.get('reason', 'nothing to heal')} — target already "
              f"{out.get('target')!r}")
        return 0
    verb = "healed" if apply else "would heal (dry run — pass --apply to write)"
    print(f"{seat_id} {verb}: target={out['target']!r}")
    for row in out.get("current_before", []):
        print(f"  before: {row['value']!r} (source={row['source_id']}, "
              f"observed={row['observed_at']})")
    return 0


async def _resolve_target_agent(actions: Any, handle_or_agent: str) -> str | None:
    """Handle or raw agent id -> a real, existing agent id — the same fallback shape
    cmd_correct_pin_value already uses (#204: shared here since three new doors below need
    the identical resolution and a terminal has no mounted identity to be self-scoped
    about, so all of them take an EXPLICIT target)."""
    from src.orchestrator.agents import resolve_handle

    agent_id = await resolve_handle(actions, handle_or_agent)
    if agent_id is not None:
        return agent_id
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
        handle_or_agent)
    return handle_or_agent if exists else None


async def cmd_correct_agent_house(
    handle_or_agent: str, *, project: str | None = None, seat_generation: int | None = None,
    actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris correct-agent-house <agent> [--project P] [--seat-generation N] [--actor W]
    — the console-script door onto orchestrator.agents.correct_agent_house, the SAME
    function the correct_agent_house MCP tool wraps (#204, the #199 lane 3B audit's
    real gap). UNLIKE correct_house (self-scoped), NOT self-scoped — the target need not
    be the caller, so this takes an EXPLICIT target, resolved the same handle-or-raw-id
    way correct-pin-value's own console door does."""
    from src.actions.core import Actions
    from src.orchestrator.agents import correct_agent_house as _correct_agent_house

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
                application_name="osiris-cli:correct-agent-house")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris correct-agent-house: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        actions = Actions(pool)
        agent_id = await _resolve_target_agent(actions, handle_or_agent)
        if agent_id is None:
            print(f"osiris correct-agent-house: refused — no such claimed seat or live "
                  f"agent: {handle_or_agent!r}", file=sys.stderr)
            return 1
        out = await _correct_agent_house(actions, agent_id=agent_id, project=project,
                                         seat_generation=seat_generation, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris correct-agent-house: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"corrected {agent_id}: {out}")
    return 0


async def cmd_reconcile_merge(
    dupe: str, into: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris reconcile-merge <dupe> <into> [--actor W] — the console-script door onto
    orchestrator.merge.reconcile_merge, the SAME function the reconcile_merge MCP tool
    wraps (#204). Repairs the estate a partial first fold left stranded on an
    ALREADY-MERGED dupe — never re-performs the merge itself (that's `merge`'s job)."""
    from src.actions.core import Actions
    from src.orchestrator.merge import reconcile_merge as _reconcile_merge

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
                application_name="osiris-cli:reconcile-merge")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris reconcile-merge: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _reconcile_merge(Actions(pool), dupe=dupe, into=into, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris reconcile-merge: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"reconciled {dupe} -> {into}: {out}")
    return 0


async def cmd_retire_agent(
    handle_or_agent: str, because: str, *, override_live: bool = False, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris retire-agent <agent> --because <reason> [--override-live] [--actor W] — the
    console-script door onto orchestrator.agents.retire_agent, the SAME function the
    retire_agent MCP tool wraps (#204). Third-party retirement, complementing the
    self-scoped `retire` (a raw terminal has no mounted session of its own to retire, so
    this always names an EXPLICIT target). ALWAYS releases the target's held seat and
    mount rows on success; refuses on a target seen live within 15 min unless
    --override-live."""
    from src.actions.core import Actions
    from src.orchestrator.agents import retire_agent as _retire_agent

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
                application_name="osiris-cli:retire-agent")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris retire-agent: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        actions = Actions(pool)
        agent_id = await _resolve_target_agent(actions, handle_or_agent)
        if agent_id is None:
            print(f"osiris retire-agent: refused — no such claimed seat or live agent: "
                  f"{handle_or_agent!r}", file=sys.stderr)
            return 1
        out = await _retire_agent(actions, agent_id=agent_id, actor=actor, because=because,
                                  override_live=override_live)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris retire-agent: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"retired {agent_id}: {out}")
    return 0


async def cmd_fleet_reconcile(
    *, execute: bool = False, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris fleet-reconcile [--execute] [--actor W] — the console-script door onto
    orchestrator.fleet_reconcile.reconcile_execute, the SAME function the fleet_reconcile
    MCP tool wraps (#204). Dry run is the default (returns the plan, writes nothing);
    --execute performs it, re-reading the tray fresh immediately before acting."""
    from src.actions.core import Actions
    from src.orchestrator.fleet_reconcile import reconcile_execute

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
                application_name="osiris-cli:fleet-reconcile")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris fleet-reconcile: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await reconcile_execute(Actions(pool), actor=actor, execute=execute)
    finally:
        if owns_pool:
            await pool.close()
    verb = "executed" if execute else "planned (dry run — pass --execute to write)"
    print(f"fleet-reconcile {verb}: {out}")
    return 0


async def cmd_fleet_prune(
    *, execute: bool = False, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris fleet-prune [--execute] [--actor W] — the console-script door onto
    orchestrator.fleet_prune.prune_execute (thread 07ca68ca, wave 8), the same function
    `agent(action='fleet_prune')` wraps. Dry run is the default (returns the plan, writes
    nothing); --execute performs it. Deliberately narrower than fleet-reconcile: only
    dead_transcript (a mount row whose own job_dir is gone from disk) and unclaimed_body
    (a live OS body bound to its seat when tree_seat_hint resolves one) — fleet-reconcile's
    own identity-folding buckets stay behind its own kill switch, untouched here."""
    from src.actions.core import Actions
    from src.orchestrator.fleet_prune import prune_execute

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
                application_name="osiris-cli:fleet-prune")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris fleet-prune: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await prune_execute(Actions(pool), actor=actor, execute=execute)
    finally:
        if owns_pool:
            await pool.close()
    verb = "executed" if execute else "planned (dry run — pass --execute to write)"
    print(f"fleet-prune {verb}: {out}")
    return 0


async def cmd_heal_seat_transcript(
    handle: str, source_paths: list[str], *, apply: bool = False, because: str = "",
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris heal-seat-transcript <handle> <source_paths...> [--because R] [--apply] —
    the console-script door onto orchestrator.transcript_splice.heal_seat_transcript, the
    SAME function the heal_seat_transcript MCP tool wraps (#204: THE ORIGINAL specimen
    this whole lane exists to prevent recurring — it shipped with no CLI door at all).
    `--apply` is required to actually write, matching every other repair verb in this
    house; dry_run reports clean/refused per pair and where the result would land."""
    from src.orchestrator.transcript_splice import heal_seat_transcript as _heal

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
                application_name="osiris-cli:heal-seat-transcript")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris heal-seat-transcript: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _heal(pool, handle, source_paths, dry_run=not apply, because=because)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris heal-seat-transcript: refused — {out['error']}", file=sys.stderr)
        return 1
    if out.get("dry_run"):
        print(f"would heal (dry run — pass --apply to write): {out}")
    else:
        print(f"healed: {out}")
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
    pool: asyncpg.Pool | None = None, office_root: Path | None = None,
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

    Prints mint_seat's own occupancy-aware `next_step_cli` (vacant: `osiris launch
    <handle>`; occupied/cold: nothing needed) rather than a second, driftable copy of
    that advice — the terminal-appropriate twin of `next_step`, which stays MCP-native
    call syntax for the `mint_seat` tool's own agent callers (thread bc11a2d3/msg 6262).

    `office_root` is TEST-ONLY plumbing (no CLI flag exposes it — a real operator never
    wants scaffolding anywhere but the standard `~/.osiris/seats/` location, so this
    stays a keyword-only escape hatch): mint_seat/_scaffold_office already accept an
    injectable office_root for exactly this, but this console door never threaded it
    through — every unmocked test-level call scaffolded a REAL office under the
    developer's real home directory while the DB side rolled back in a test transaction,
    leaving a directory on disk with no matching Seat (climintworker1/inferredworker1,
    Thoth's msg 3928/6026 — an office with no Seat, the exact inverse of #139's mint-door
    catalog, manufactured by our own test suite on every real run)."""
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
        if office_root is not None:
            kwargs["office_root"] = office_root
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
        # THE PROJECT CONFESSION, same voice as `osiris new`'s own (decision 24e0b761):
        # only fires when this call actually WROTE the pin (`project_declared is None`
        # means the pin already existed and was left untouched — nothing new to confess).
        if office.get("osiris_pin_project_declared") is False:
            print("project: unset in this office's pin — no --project given, none "
                  "invented. mount will fill this in on its own once the graph "
                  "unambiguously knows it; `osiris mint-seat ... --project <name>` "
                  "declares it now.")
    print(f"model: {out['intended_model']}"
          + (" (stamped)" if out.get("intended_model_stamped") else ""))
    print(f"manager: {out['manager_seat_id']} ({out['managed_by']})")
    print(f"occupancy: {out['occupancy']} — {out['next_step_cli']}")
    return 0


# --- new -----------------------------------------------------------------------------------------

async def cmd_new(
    handle: str, path: str | None, *, project: str | None, house: str | None,
    model: str | None, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris new <handle> [path] [--project P] [--house H] [--model M] [--actor <who>] —
    ONE command, no ceremony (dispatch 3685/3688, the operator's own "too much witchcraft to
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

    # THE CONFESSION, BEFORE ANYTHING IS WRITTEN (thread bc11a2d3/msg 6262, the operator's
    # real transcript: `mkdir cdking && cd cdking && osiris new Chad` silently created
    # ~/code/chad instead — cdking is now an orphan). Never silently switches the default
    # to cwd (deriving-by-convention is the exact trap the earlier anchor_cwd bug came
    # from) — only names both paths and the exact remedy, so the operator decides.
    if path is None:
        cwd = Path.cwd()
        default_workspace = Path.home() / "code" / handle.lower()
        if cwd != Path.home() and cwd != default_workspace:
            print(f"osiris new: standing in {cwd}, but no path given — this creates "
                  f"{default_workspace} instead (osiris never assumes your cwd is the "
                  f"workspace). To use where you are: osiris new {handle} .",
                  file=sys.stderr)

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
                                house=house, actor=actor, **kwargs)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris new: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"{'founded' if out['seat_minted'] else 'converged on'} {out['handle']} "
          f"({out['seat_id']}) — self-managed, no manager")
    # THE PROJECT CONFESSION (the operator, live, 2026-09-02: "the thing cannot handle
    # 'no project' — it falsely creates a jesus project and a chad project when really
    # they are working somewhere else", decision 24e0b761): no --project no longer
    # fabricates one from the handle — say so plainly, the same "confess, never assume"
    # voice as the cwd-default note above, rather than leaving a silent `project: None`
    # for the next reader to puzzle over.
    if out["project"]:
        print(f"project: {out['project']}")
    else:
        print("project: unset — no --project given, none invented. mount will fill "
              "this in on its own once the graph unambiguously knows it; "
              "`osiris new <handle> --project <name>` declares it now.")
    if out["house"]:
        print(f"house: {out['house']}")
    else:
        print("house: unset — no --house given, none invented (ruling 68fba2e4: homeless "
              "is a legal state, never fabricated from the handle). "
              "`osiris new <handle> --house <name>` declares it now.")
    print(f"workspace: {out['workspace']} ({out['workspace_pin']})")
    office = out.get("office")
    if office:
        print(f"office: {office['office']} (pin {office['osiris_pin']}, orders "
              f"{office['standing_orders']}, charter {office['charter_file']})")
    # THE CASE NOTE (thread bc11a2d3/msg 6262): resolution is case-insensitive by design
    # — this is not a bug — but a caller who typed 'Chad' and sees 'chad' in every path
    # with nothing saying so reads it as unpredictable. Say plainly which is which,
    # only when they actually differ.
    if out["handle"] != out["handle"].lower():
        print(f"note: paths use the lowercase form ({out['handle'].lower()!r}); the "
              f"handle itself keeps your capitalization ({out['handle']!r}) — both name "
              "the same seat")
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


# ============================================================================================
# WAVE 3 (thread 5bf6447c, Thoth dispatch 7943): the 25 NO_CLI_EQUIVALENT excuses re-read
# one by one against today's code. These sixteen had a real, standalone, third-party-capable
# orchestrator function all along — the console door was simply never scoped (each earlier
# entry's own generic "not on the jesus/chad path; not ruled out" reason, from a narrower
# dispatch that never claimed these were impossible, only out of scope). Every one below
# calls the SAME function its own MCP tool wraps — verified by reading each tool's own
# forwarding body in src/mcp_server.py before writing its CLI twin, never guessed from the
# orchestrator module alone (a hidden alias's own MCP-facing param names, not the internal
# orchestrator function's, are the parity contract this file's forward detector checks).
# ============================================================================================


async def cmd_attach_seat(
    worker: str, manager: str, evidence: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris attach-seat <worker> <manager> <evidence> [--actor W] — the console-script
    door onto orchestrator.seats.attach_seat, the SAME function the attach_seat MCP tool
    wraps (forwards to seat_edge(action='attach')). Creates a managed_by edge."""
    from src.actions.core import Actions
    from src.orchestrator.seats import attach_seat as _attach_seat

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
                application_name="osiris-cli:attach-seat")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris attach-seat: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _attach_seat(Actions(pool), worker, manager, evidence=evidence,
                                 actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris attach-seat: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"attached {worker} -> {manager}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_detach_seat(
    seat: str, because: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris detach-seat <seat> <because> [--actor W] — the console-script door onto
    orchestrator.seats.detach_seat, the SAME function the detach_seat MCP tool wraps
    (forwards to seat_edge(action='detach')). Invalidates an active managed_by edge."""
    from src.actions.core import Actions
    from src.orchestrator.seats import detach_seat as _detach_seat

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
                application_name="osiris-cli:detach-seat")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris detach-seat: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _detach_seat(Actions(pool), seat, because=because, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris detach-seat: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"detached {seat}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_promote(
    target: str, workers: list[str], because: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris promote <target> <worker1> [worker2 ...] --because <str> [--actor W] — the
    console-script door onto orchestrator.seats.promote_seat, the SAME function the seat
    MCP tool's action='promote' branch wraps. Mints target as manager over each worker
    (peer bonds invalidated, house derived, offices reissued), one transaction, per-worker
    outcomes never a whole-call failure. `--actor` defaults to `_CONSOLE_ACTOR`
    ('console'), one of promote_seat's own recognized operator sentinels — a bare
    terminal invocation IS the operator's own hand by construction, same authority every
    other third-party seat-write CLI door in this file already carries."""
    from src.actions.core import Actions
    from src.orchestrator.seats import promote_seat as _promote_seat

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
                application_name="osiris-cli:promote")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris promote: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _promote_seat(Actions(pool), target, workers, because=because,
                                  actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris promote: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"promoted {out['promoted']} over {len(workers)} worker(s)")
    for worker, verdict in out.get("workers", {}).items():
        print(f"  {worker}: {verdict}")
    return 0


async def cmd_vacate_seat(
    seat_id: str, because: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris vacate-seat <seat> <because> [--actor W] — the console-script door onto
    orchestrator.trigger.vacate_dead_seat, the SAME function the vacate_seat MCP tool
    wraps (forwards to seat(action='vacate')). Releases a dead holder without retiring
    the seat itself."""
    from src.actions.core import Actions
    from src.orchestrator.trigger import vacate_dead_seat

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
                application_name="osiris-cli:vacate-seat")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris vacate-seat: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await vacate_dead_seat(Actions(pool), seat_id=seat_id, actor=actor,
                                     because=because)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris vacate-seat: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"vacated {seat_id}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_retire_seat(
    seat_id: str, reason: str = "", *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris retire-seat <seat> [--reason R] [--actor W] — the console-script door onto
    orchestrator.seats.retire_seat, the SAME function the retire_seat MCP tool wraps
    (forwards to retire_object(kind='seat')). Marks a Seat permanently CLOSED."""
    from src.actions.core import Actions
    from src.orchestrator.seats import retire_seat as _retire_seat

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
                application_name="osiris-cli:retire-seat")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris retire-seat: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _retire_seat(Actions(pool), seat_id, reason=reason, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris retire-seat: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"retired {seat_id}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_bind_seat_tree(
    seat_id: str, tree_cwd: str, because: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris bind-seat-tree <seat> <tree_cwd> <because> [--actor W] — the console-script
    door onto orchestrator.seats.bind_seat_tree, the SAME function the bind_seat_tree MCP
    tool wraps (forwards to seat(action='bind_tree')). Points a seat's CODE checkout,
    distinct from its anchor office."""
    from src.actions.core import Actions
    from src.orchestrator.seats import bind_seat_tree as _bind_seat_tree

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
                application_name="osiris-cli:bind-seat-tree")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris bind-seat-tree: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _bind_seat_tree(Actions(pool), seat_id=seat_id, tree_cwd=tree_cwd,
                                    actor=actor, because=because)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris bind-seat-tree: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"bound {seat_id} tree -> {tree_cwd}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_sweep_seat_disk(
    handle: str, dry_run: bool = True, because: str = "",
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris sweep-seat-disk <handle> [--apply] [--because R] — the console-script door
    onto orchestrator.offices.sweep_retired_office + sweep_seat_workspace, the SAME two
    functions the sweep_seat_disk MCP tool wraps (forwards to seat(action='sweep_disk')).
    Dry-run by default; --apply writes."""
    from src.orchestrator.offices import sweep_retired_office, sweep_seat_workspace

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
                application_name="osiris-cli:sweep-seat-disk")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris sweep-seat-disk: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    because_arg = because.strip() or None
    try:
        office_out = await sweep_retired_office(pool, handle=handle, dry_run=dry_run,
                                                because=because_arg)
        workspace_out = await sweep_seat_workspace(pool, handle=handle, dry_run=dry_run,
                                                   because=because_arg)
    finally:
        if owns_pool:
            await pool.close()
    verb = "swept" if not dry_run else "would sweep (dry run — pass --apply to write)"
    print(f"{handle} {verb}:")
    print(f"  office: {office_out}")
    print(f"  workspace: {workspace_out}")
    return 0


async def cmd_rename_seat(
    seat_id: str, new_handle: str, because: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris rename-seat <seat> <new_handle> <because> [--actor W] — the console-script
    door onto orchestrator.seats.rename_seat, the SAME function the rename_seat MCP tool
    wraps (forwards to seat(action='rename'))."""
    from src.actions.core import Actions
    from src.orchestrator.seats import rename_seat as _rename_seat

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
                application_name="osiris-cli:rename-seat")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris rename-seat: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _rename_seat(Actions(pool), seat_id=seat_id, new_handle=new_handle,
                                 actor=actor, because=because)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris rename-seat: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"renamed {seat_id} -> {new_handle}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_set_seat_attended(
    seat_id: str, attended: str, because: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris set-seat-attended <seat> <attended> <because> [--actor W] — the console-
    script door onto orchestrator.seats.set_seat_attended, the SAME function the
    set_seat_attended MCP tool wraps (forwards to seat(action='set_attended'))."""
    from src.actions.core import Actions
    from src.orchestrator.seats import set_seat_attended as _set_seat_attended

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
                application_name="osiris-cli:set-seat-attended")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris set-seat-attended: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _set_seat_attended(Actions(pool), seat_id=seat_id, attended=attended,
                                       actor=actor, because=because)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris set-seat-attended: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"{seat_id} attended -> {attended}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_reissue_office(
    seat_id: str, because: str, *, adopt: bool = False, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris reissue-office <seat> <because> [--adopt] [--actor W] — the console-script
    door onto orchestrator.boot_compiler.reissue_office, the SAME function the
    reissue_office MCP tool wraps (forwards to seat(action='reissue_office'))."""
    from src.actions.core import Actions
    from src.orchestrator.boot_compiler import reissue_office as _reissue_office

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
                application_name="osiris-cli:reissue-office")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris reissue-office: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _reissue_office(Actions(pool), seat_id=seat_id, because=because,
                                    actor=actor, adopt=adopt)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris reissue-office: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"reissued office for {seat_id}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_establish_office(
    seat: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris establish-office <seat> [--actor W] — the console-script door onto
    orchestrator.offices.establish_office, the SAME function the establish_office MCP
    tool wraps (forwards to seat(action='establish_office')). The full office ceremony,
    one receipt."""
    from src.actions.core import Actions
    from src.orchestrator.offices import establish_office as _establish_office

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
                application_name="osiris-cli:establish-office")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris establish-office: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _establish_office(Actions(pool), seat_or_agent=seat, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris establish-office: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"established office for {seat}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_resync_seat_house(
    seat_id: str, new_house: str | None, reason: str, *, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris resync-seat-house <seat> <new_house|--none> <reason> [--actor W] — the
    console-script door onto orchestrator.seats.resync_seat_house_third_party, the SAME
    function the resync_seat_house MCP tool wraps (forwards to
    seat(action='resync_house')). `new_house=None` unsets a redundant house third-party
    (thread dc1b5a20's own door note: this, never retire_assertion, is how a house third-
    party is genuinely unset)."""
    from src.actions.core import Actions
    from src.orchestrator.seats import resync_seat_house_third_party

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
                application_name="osiris-cli:resync-seat-house")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris resync-seat-house: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await resync_seat_house_third_party(
            Actions(pool), seat_id, new_house, source=actor, reason=reason)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris resync-seat-house: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"{seat_id} house -> {new_house!r}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_reconcile_seat_identity(
    seat_id: str, because: str, *, agent_id: str | None = None, actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris reconcile-seat-identity <seat> <because> [--agent-id A] [--actor W] — the
    console-script door onto orchestrator.identity_heal.reconcile_seat_identity_third_
    party, the SAME function reconcile_seat_identity_third_party (and, third-party-wise,
    reconcile_seat_identity) MCP-forwards to (seat(action='reconcile_identity')). Always
    third-party here — a raw terminal has no mounted identity to reconcile self-wise."""
    from src.actions.core import Actions
    from src.orchestrator.identity_heal import reconcile_seat_identity_third_party

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
                application_name="osiris-cli:reconcile-seat-identity")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris reconcile-seat-identity: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await reconcile_seat_identity_third_party(
            Actions(pool), seat_id=seat_id, agent_id=agent_id, because=because,
            actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris reconcile-seat-identity: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"reconciled {seat_id}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_create_project(
    name: str, because: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris create-project <name> <because> [--actor W] — the console-script door onto
    orchestrator.project_identity.create_project, the SAME function the create_project
    MCP tool wraps (forwards to project(action='create'))."""
    from src.actions.core import Actions
    from src.orchestrator.project_identity import create_project as _create_project

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
                application_name="osiris-cli:create-project")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris create-project: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _create_project(Actions(pool), name=name, because=because, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris create-project: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"created project {name!r}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_rename_project(
    project: str, new_name: str, because: str, *, dry_run: bool = True,
    merge_into: bool = False, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris rename-project <project> <new_name> <because> [--apply] [--merge-into]
    [--actor W] — the console-script door onto orchestrator.project_identity.
    rename_project, the SAME underlying write the rename_project MCP tool's
    project(action='rename') calls. Dry-run by default; --apply writes.

    RECEIPT LAW (Thoth mail 9122 item 1, wave 16) — NOT the same overall guarantee as
    the MCP door, and this docstring used to falsely claim it was: the MCP door also
    (a) gathers governing-seat evidence and warns when it disagrees with `new_name`
    (`rename_evidence`/`evidence_disagrees`/`warning`, mcp_server.py's `_project_impl`
    rename branch) and (b) heals every already-mounted agent's IN-PROCESS mount cache
    so a live session's `get_status()` stops reporting the pre-rename name. (b) is
    structurally inapplicable here — a CLI invocation is its own short-lived process
    with no `_agents` cache to heal; only the long-lived MCP server process has one.
    (a) is portable and IS run here now (see below) — the earlier gap was landing a
    rename while governing-seat evidence still disagreed with the new name, with no
    warning at all, silently."""
    from src.actions.core import Actions
    from src.orchestrator.project_identity import project_identity_evidence, rename_evidence_verdict
    from src.orchestrator.project_identity import rename_project as _rename_project
    from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project

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
                application_name="osiris-cli:rename-project")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris rename-project: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        # SAME evidence-gathering the MCP door runs, before the write (best-effort —
        # an ambiguous ref is the real refusal inside _rename_project itself below).
        evidence_by_seat: dict[str, Any] = {}
        try:
            row = await _resolve_software_project(pool, project)
        except AmbiguousProjectRef:
            row = None
        if row is not None:
            seat_rows = await pool.fetch(
                "SELECT s.canonical FROM links l JOIN objects s ON s.id=l.from_id "
                "WHERE l.to_id=$1 AND l.type='governs' "
                "AND (l.valid_until IS NULL OR l.valid_until > now())", row["id"])
            for r in seat_rows:
                evidence_by_seat[r["canonical"]] = await project_identity_evidence(
                    pool, seat_id=r["canonical"])
        out = await _rename_project(Actions(pool), project=project, new_name=new_name,
                                    because=because, actor=actor, dry_run=dry_run,
                                    merge_into=merge_into)
        if evidence_by_seat and not out.get("error") and not dry_run:
            rename_evidence = {
                seat: {"verdict": rename_evidence_verdict(ev, new_name), "evidence": ev}
                for seat, ev in evidence_by_seat.items()
            }
            out["rename_evidence"] = rename_evidence
            disagreeing = [s for s, v in rename_evidence.items() if v["verdict"] == "disagrees"]
            if disagreeing:
                out["evidence_disagrees"] = True
                out["warning"] = (
                    f"{new_name!r} was written, but {len(disagreeing)} governing seat "
                    f"evidence disagrees with it: {', '.join(disagreeing)} — their own "
                    "pin/charter/remote still names something else; go fix those, this "
                    "write did not")
        if not dry_run and not out.get("error"):
            out["mount_cache_note"] = (
                "any already-mounted agent's in-process get_status() may still report "
                "the pre-rename name until its next re-mount — this CLI process has no "
                "live agent cache to heal (only the MCP server process does)")
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris rename-project: refused — {out['error']}", file=sys.stderr)
        return 1
    verb = "renamed" if not dry_run else "would rename (dry run — pass --apply to write)"
    print(f"{project} {verb} -> {new_name}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_set_project_tag(
    project: str, tag: str, because: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris set-project-tag <project> <tag> <because> [--actor W] — the console-script
    door onto orchestrator.projects.set_project_window_tag, the SAME function the
    project(action='set_tag') MCP verb wraps. Declares the persisted `[TAG]` override
    trigger.py's `_house_tag`/`_window_name` read BEFORE ever deriving one from the
    house/project's own first two letters."""
    from src.actions.core import Actions
    from src.orchestrator.projects import set_project_window_tag as _set_project_window_tag

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
                application_name="osiris-cli:set-project-tag")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris set-project-tag: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _set_project_window_tag(Actions(pool), project=project, tag=tag,
                                            because=because, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris set-project-tag: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"{project}: window tag set to [{out['window_tag']}]")
    return 0


async def cmd_retire_project(
    project: str, because: str, *, actor: str, pool: asyncpg.Pool | None = None,
) -> int:
    """osiris retire-project <project> <because> [--actor W] — the console-script door
    onto orchestrator.projects.retire_project, the SAME function the retire_project MCP
    tool wraps (forwards to retire_object(kind='project'))."""
    from src.actions.core import Actions
    from src.orchestrator.projects import retire_project as _retire_project

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
                application_name="osiris-cli:retire-project")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris retire-project: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        out = await _retire_project(Actions(pool), project=project, actor=actor,
                                    because=because)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris retire-project: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"retired project {project!r}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0


async def cmd_fork_project(
    project: str, fork_into: str, because: str, *, direction: str = "fork", actor: str,
    pool: asyncpg.Pool | None = None,
) -> int:
    """osiris fork-project <project> <fork_into> <because> [--direction fork|unfork]
    [--actor W] — the console-script door onto orchestrator.project_identity.
    fork_project, the SAME function the fork_project MCP tool wraps (forwards to
    project(action='fork'/'unfork') by direction)."""
    from src.actions.core import Actions
    from src.orchestrator.project_identity import fork_project as _fork_project

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
                application_name="osiris-cli:fork-project")
        except Exception as exc:  # noqa: BLE001 - the CLI boundary: report, no raw traceback
            print(f"osiris fork-project: could not reach postgres at "
                  f"{settings.database_url} — {exc}. Set DATABASE_URL, or start the dev "
                  "instance.", file=sys.stderr)
            return 1
    try:
        if direction == "unfork":
            from src.orchestrator.project_identity import unfork_project as _unfork
            out = await _unfork(Actions(pool), project=project, fork_into=fork_into,
                                because=because, actor=actor)
        else:
            out = await _fork_project(Actions(pool), project=project, fork_into=fork_into,
                                      because=because, actor=actor)
    finally:
        if owns_pool:
            await pool.close()
    if "error" in out:
        print(f"osiris fork-project: refused — {out['error']}", file=sys.stderr)
        return 1
    print(f"{project} {direction} -> {fork_into}")
    for k, v in out.items():
        print(f"  {k}: {v}")
    return 0

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
  start a mind          new, launch, resume, mint-seat, attach
  end one               stop
  see the fleet         fleet, roster, backlog, team, status, boot-status, smoke, lint,
                        audit
  read the record       desk, show, threads, inbox, search
  write to the record   send, decide, thread, annotate-thread, amend-decision,
                        charter-for, amend-practice, merge, unmerge, fold-project,
                        rebind-seat, correct-pin-value, heal-seat-anchor,
                        transition-seat-project, correct-agent-house, reconcile-merge,
                        retire-agent, heal-seat-transcript, attach-seat, detach-seat,
                        promote, vacate-seat, retire-seat, bind-seat-tree, sweep-seat-disk,
                        rename-seat, set-seat-attended, reissue-office,
                        establish-office, resync-seat-house, reconcile-seat-identity,
                        create-project, rename-project, retire-project, fork-project,
                        set-project-tag, proposal, settings
  operate               deploy, migrate, seed, bootstrap, retention, rematerialize,
                        fleet-reconcile, fleet-prune

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
    p_attach.add_argument("handle", help="the seat's handle to attach to")

    p_smoke = sub.add_parser(
        "smoke", description=_d("the same deploy-time liveness probe the fleet runs"),
        epilog="example: osiris smoke\nexample: osiris smoke --chaos")
    p_smoke.add_argument(
        "--chaos", action="store_true",
        help="crash replay: kill osiris-mcp/osiris-worker hard, fire a concurrent "
             "session-end storm, restart, then assert system invariants still hold "
             "under a real crash, never just a graceful restart")
    p_smoke.add_argument("--json", action="store_true", dest="as_json",
                         help="machine-readable: one compact JSON line. Never chaos's "
                              "own path (--chaos runs a separate, longer-lived probe "
                              "with its own text receipt) — the ordinary probe only")

    p_boot_status = sub.add_parser("boot-status", description=_d(
        "name every active seat with no compiled managed section, "
                               "classified by why (report-only; exit 1 if any)"),
                   epilog="example: osiris boot-status")
    p_boot_status.add_argument("--json", action="store_true", dest="as_json",
                               help="machine-readable: one compact JSON line")

    p_lint = sub.add_parser("lint", description=_d(
        "the graph audits itself — headless mirror of the graph_lint MCP tool/CMD-K "
        "power tool, same 32 checks, same receipt (report-only; exit 1 if any findings)"),
                   epilog="example: osiris lint\nexample: osiris lint --check false-mint "
                          "--json\nexample: osiris lint --project osiris")
    p_lint.add_argument("--check", default=None,
                        help="one check name (a `counts` key) to list in full, past the "
                             "default 50-per-check cap — see --json's own counts for the "
                             "complete name list")
    p_lint.add_argument("--project", default=None,
                        help="best-effort client-side filter (no SQL-level scoping exists "
                             "upstream) — keeps only findings whose subject/detail "
                             "mentions this string; --json names which checks could not "
                             "be evaluated for project membership at all")
    p_lint.add_argument("--json", action="store_true", dest="as_json",
                        help="machine-readable: the full receipt (findings/counts/"
                             "counts_by_severity/severity/could_not_evaluate)")
    p_lint.add_argument("--stale-days", type=int, default=14, dest="stale_days",
                        help="the stale-obligation/rot-candidate/peer-silent window, days "
                             "(default 14)")
    p_lint.add_argument("--limit", type=int, default=None,
                        help="with --check: page size past the default full fetch")
    p_lint.add_argument("--offset", type=int, default=0,
                        help="with --check: page offset")

    p_audit = sub.add_parser("audit", description=_d(
        "headless mirror of graph_lint's own CMD-K audit siblings — one door for all "
        "five rather than a subcommand each"),
                   epilog="example: osiris audit the-wall\nexample: osiris audit "
                          "closure-health --json")
    p_audit.add_argument("name", choices=list(AUDIT_NAMES),
                         help="which audit to run")
    p_audit.add_argument("--json", action="store_true", dest="as_json",
                         help="machine-readable: the full composition result")

    p_seed = sub.add_parser("seed", description=_d("seed default compositions (and rooms)"),
                            epilog="example: osiris seed\nexample: osiris seed "
                                   "--compositions-only")
    p_seed.add_argument("--compositions-only", action="store_true",
                        help="seed + room DEFAULT_COMPOSITIONS only; skip the canon ingest")

    p_launch = sub.add_parser("launch", description=_d(
        "body a seat with a fresh, persistent `claude --bg` process — always shows up "
        "in `claude agents`, always attachable. To continue the seat's last session "
        "instead, use `osiris resume`"),
                              epilog="example: osiris launch Khnum")
    p_launch.add_argument("handle", help="the seat's handle to launch a body for")
    p_launch.add_argument("--model", default=None,
                          help="the model to launch with — defaults to the seat's own "
                               "recorded intended_model, else the fleet's wake default")
    p_launch.add_argument("--debug", action="store_true",
                          help="use the osiris PTY-broker lane instead of the default "
                               "`claude --bg` — for an incident or a build with no --bg")

    p_resume = sub.add_parser("resume", description=_d(
        "continue a seat's last session as a ONE-SHOT `-p --resume` turn — runs the "
        "brief and exits, never shows up in `claude agents`, reached again by sending "
        "it mail. Refuses if there is nothing resumable; never falls through to a "
        "fresh mint — for that, use `osiris launch`"),
                              epilog="example: osiris resume Khnum")
    p_resume.add_argument("handle", help="the seat's handle to resume")
    p_resume.add_argument("--model", default=None,
                          help="the model to resume with — defaults to the seat's own "
                               "recorded intended_model, else the fleet's wake default")

    p_stop = sub.add_parser("stop", description=_d(
        "END a live body — `osiris launch`'s inverse. Stops the seat's own process (the "
        "harness's own `claude stop <id>` when the body is harness-tracked, else a plain "
        "SIGTERM) and nothing more: not a pause, no promised thaw-where-you-left-off. "
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

    p_status = sub.add_parser("status", description=_d(
        "your identity, mail count, and fleet pulse — the same get_status() the MCP tool "
        "answers, called over the wire"),
        epilog="example: osiris status")
    p_status.add_argument("--json", action="store_true", dest="as_json",
                          help="machine-readable: one compact JSON line, for a script or an agent")

    p_search = sub.add_parser("search", description=_d(
        "search the graph's knowledge — the same search() the MCP tool answers, called "
        "over the wire"),
        epilog="example: osiris search 'quorumlatch counter'")
    p_search.add_argument("query", help="words, phrases, or \"quoted phrases\" (websearch syntax)")
    p_search.add_argument("--limit", type=int, default=15, help="max results (default 15)")
    p_search.add_argument("--json", action="store_true", dest="as_json",
                          help="machine-readable: one compact JSON line, for a script or an agent")

    p_fleet = sub.add_parser("fleet", description=_d(
        "the fleet roster, grouped by project — the same "
                                         "fleet() the MCP tool answers, called over the wire"),
                             epilog="example: osiris fleet\nexample: osiris fleet --full")
    p_fleet.add_argument("--full", action="store_true",
                         help="expand every historical (retired) session too, not just the "
                              "live ones — the whole roster, not the collapsed count")
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
    p_roster.add_argument("--caveats", action="store_true", dest="want_caveats",
                          help="the full text of this function's own blind spots (default: "
                               "a one-line count+pointer, same diet as the MCP tool)")
    p_roster.add_argument("--json", action="store_true", dest="as_json",
                          help="machine-readable: one compact JSON line, for a script or an agent")

    p_backlog = sub.add_parser("backlog", description=_d(
        "per-project open obligations against target, past-window first, oldest owners — "
        "the same backlog() the MCP tool answers, called over the wire. Scoped to your own "
        "mounted project by default"),
        epilog="example: osiris backlog\nexample: osiris backlog --all-projects"
               "\nexample: osiris backlog --fleet")
    p_backlog.add_argument("--all-projects", action="store_true", dest="all_projects",
                           help="every project, not just your own mounted one")
    p_backlog.add_argument("--fleet", action="store_true", dest="fleet",
                           help="the per-seat crunch view instead of per-project "
                                "(thread 8608, THE BACKLOG BAND) — who is carrying the "
                                "backlog fleet-wide, takes priority over --all-projects")
    p_backlog.add_argument("--json", action="store_true", dest="as_json",
                           help="machine-readable: one compact JSON line, for a script or an agent")

    p_threads = sub.add_parser("threads", description=_d(
        "MINE: every OPEN thread you own, one line each with a short id — the same "
        "threads() the MCP tool answers, called over the wire. A raw terminal has no "
        "mounted identity of its own, so --project is effectively required here"),
        epilog="example: osiris threads --project osiris")
    p_threads.add_argument("--project", default=None,
                           help="the repo to list open threads for")
    p_threads.add_argument("--json", action="store_true", dest="as_json",
                           help="machine-readable: one compact JSON line, for a script or an agent")

    p_team = sub.add_parser("team", description=_d(
        "a manager's own seats: live, owe, envelope — the same team() the MCP tool "
        "answers, called over the wire (self-scoped off the caller's own held seat). "
        "--seat resolves the manager by handle directly, off the wire, for a bare "
        "terminal with no mount of its own"),
        epilog="example: osiris team\nexample: osiris team --seat Thoth")
    p_team.add_argument("--seat", default=None,
                        help="the manager's own handle (bypasses the MCP self-scoping gap)")
    p_team.add_argument("--json", action="store_true", dest="as_json",
                        help="machine-readable: one compact JSON line, for a script or an agent")

    p_inbox = sub.add_parser("inbox", description=_d(
        "a peek at a project's own mailbox — the same inbox() the MCP tool answers, "
        "called over the wire. Always a peek; settling mail is an agent's own act"),
        epilog="example: osiris inbox --project osiris")
    p_inbox.add_argument("--project", required=True, help="the project mailbox to peek at")
    p_inbox.add_argument("--json", action="store_true", dest="as_json",
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
    p_merge.add_argument("--force", action="store_true",
                         help="override decision 7fe20cc5's liveness guard "
                              "(SoftwareProject folds only) — required alongside "
                              "--because to fold a project a DIFFERENT lineage's live "
                              "session currently has mounted; self never needs this")
    p_merge.add_argument("--because", default="",
                         help="required when --force is used — why the live-session "
                              "guard is being overridden")

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
            "to delete, in batches."),
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
    p_fold_project.add_argument("--force", action="store_true",
                                help="override decision 7fe20cc5's liveness guard — "
                                     "required alongside --because")
    p_fold_project.add_argument("--because", default="",
                                help="required when --force is used")

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
    p_charter_for.add_argument("--ruling", default=None,
                               help="a standing operator ruling's decision id — lets a "
                                    "non-manager act under that ruling's authority "
                                    "instead, refused unless the ruling actually names "
                                    "charter_for")

    p_settings = sub.add_parser("settings", description=_d(
        "THE SETTINGS MENU's own CLI door (thread f4498ab304e4) — list/get/set over "
        "the settings registry, the same functions the `settings` MCP tool wraps"),
        epilog="example: osiris settings list\n"
               "example: osiris settings get daemon.pit_watch.enabled\n"
               "example: osiris settings set daemon.pit_watch.enabled true "
               "--because 'watching a live pit tonight'")
    p_settings.add_argument("action", choices=("list", "get", "set"))
    p_settings.add_argument("key", nargs="?", default=None,
                            help="required for get/set — a registered dotted key "
                                 "(see 'osiris settings list')")
    p_settings.add_argument("value", nargs="?", default=None,
                            help="set only — a JSON value ('true', '5', '\"text\"', "
                                 "'{\"a\":1}'), or a bare string when it isn't valid JSON")
    p_settings.add_argument("--because", default="",
                            help="required unless the setting opts out "
                                 "(requires_because=False)")
    p_settings.add_argument("--ruling", default=None,
                            help="a standing operator ruling's decision id — lets a "
                                 "non-operator write under that ruling's authority")
    p_settings.add_argument("--scope-id", default="", dest="scope_id",
                            help="for a project/seat-scoped setting; box-scope settings "
                                 "(everything registered today) ignore this")
    p_settings.add_argument("--actor", default=_CONSOLE_ACTOR,
                            help=f"who is making this change — defaults to {_CONSOLE_ACTOR!r}")
    p_settings.add_argument("--json", action="store_true", dest="as_json",
                            help="machine-readable: one compact JSON line")

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

    p_send = sub.add_parser("send", description=_d(
        "message the fleet — the same send the MCP tool wraps, exposed as a bare-"
        "terminal door. `--to`=<project> is a BROADCAST; `--to-agent`=<agent> is a "
        "private DM. Refuses a project nobody has mounted under, or a broadcast whose "
        "body names a real seat mounted in a different room, exactly as the MCP tool "
        "does"),
        epilog="example, a broadcast: osiris send 'deploy landing' --to osiris\n"
               "example, a DM: osiris send 'ship it' --to-agent agent:abc123")
    p_send.add_argument("body", help="the message text")
    p_send.add_argument("--to", default=None, help="broadcast to this project's room")
    p_send.add_argument("--to-agent", default=None, help="DM this agent id or live handle")
    p_send.add_argument("--reply-to", type=int, default=None,
                        help="the message id this answers")
    p_send.add_argument("--desk", default=None, choices=["decision", "hands", "fyi"],
                        help="the operator-desk band this belongs to")
    p_send.add_argument("--grade", default=None, choices=["ask", "fyi"],
                        help="'ask' (named in the recipient's unread count) or 'fyi'")
    p_send.add_argument("--require-seat", action="store_true",
                        help="refuse rather than DM an unclaimed target")
    p_send.add_argument("--threads", nargs="*", default=None,
                        help="existing Thread ref(s) to transfer ownership of to a DM's "
                             "addressee")
    p_send.add_argument("--want-prior-art", action="store_true",
                        help="run the same prior-art search record_decision does")
    p_send.add_argument("--want-listener", action="store_true",
                        help="include liveness in the receipt")
    p_send.add_argument("--from-project", default=None,
                        help="CLI-only (see CLI_ONLY_PARAMS): an agent's from_project "
                             "comes from its own mount; a console caller has none, so "
                             "name it explicitly for broadcast-reply routing")
    p_send.add_argument("--actor", default=_CONSOLE_ACTOR,
                        help=f"who this is from — defaults to {_CONSOLE_ACTOR!r}")
    p_send.add_argument("--json", action="store_true", dest="as_json",
                        help="machine-readable: one compact JSON line")

    p_decide = sub.add_parser("decide", description=_d(
        "record a decision (ruling|reset|override|rejection|choice) — the same "
        "record_decision the MCP tool wraps, exposed as a bare-terminal door. An exact "
        "repeat re-uses the existing decision rather than minting a twin"),
        epilog="example: osiris decide 'freeze non-critical merges after Thursday' "
               "--rationale 'mobile team cutting a release branch' --repo osiris")
    p_decide.add_argument("summary", help="the decision, one clear sentence")
    p_decide.add_argument("--kind", default="ruling",
                          help="ruling|reset|override|rejection|choice (default: ruling)")
    p_decide.add_argument("--rationale", default=None, help="the WHY behind the summary")
    p_decide.add_argument("--repo", default=None, help="the project this decision governs")
    p_decide.add_argument("--grounds", nargs="*", default=None,
                          help="refs this decision rests on")
    p_decide.add_argument("--protocol", default=None,
                          help="the exact invocation to rerun this decision's own act")
    p_decide.add_argument("--supersedes", default=None, help="bury an earlier decision")
    p_decide.add_argument("--resolves", nargs="*", default=None,
                          help="close the Thread(s) this settles")
    p_decide.add_argument("--obsoletes", nargs="*", default=None,
                          help="kill a named Superstition")
    p_decide.add_argument("--confirms", nargs="*", default=None, help="witness a Practice")
    p_decide.add_argument("--refutes", default=None, help="disprove a Practice")
    p_decide.add_argument("--implements", default=None,
                          help="execute a standing Decision (parent stays alive)")
    p_decide.add_argument("--rediscovers", nargs="*", default=None,
                          help="independent re-arrival at an earlier decision")
    p_decide.add_argument("--bears-on", nargs="*", default=None,
                          help="speak to an open Thread without closing it")
    p_decide.add_argument("--narrows", nargs="*", default=None,
                          help="scope-bound an earlier decision")
    p_decide.add_argument("--cites", nargs="*", default=None,
                          help="add a facet to an earlier decision")
    p_decide.add_argument("--ack-prior-art", action="store_true",
                          help="record a dismissed prior_art_flag instead of a silent shrug")
    p_decide.add_argument("--unlinked-because", default=None,
                          help="a real reason through declare-or-refuse's link-kind gate")
    p_decide.add_argument("--operator-authorized", action="store_true",
                          help="this decision carries the operator's own authority — "
                               "mints a ruled_by edge to the operator's Person "
                               "object")
    p_decide.add_argument("--actor", default=_CONSOLE_ACTOR,
                          help=f"who is deciding — defaults to {_CONSOLE_ACTOR!r}")
    p_decide.add_argument("--json", action="store_true", dest="as_json",
                          help="machine-readable: one compact JSON line")

    p_thread = sub.add_parser("thread", description=_d(
        "resolve (close) a Thread — the same `thread` MCP tool's own action='resolve' "
        "branch, exposed as a bare-terminal door. A DELIBERATE NARROWING: the other "
        "three actions (annotate/correct_summary/reclassify) have no door here — "
        "annotate already has its own (osiris annotate-thread)"),
        epilog="example: osiris thread a1b2c3d4 --because 'shipped in e74efd6'\n"
               "example, closing several at once: osiris thread a1b2 c3d4 e5f6 "
               "--because 'superseded by the census' --no-dry-run")
    p_thread.add_argument("ref", nargs="+",
                          help="one or more target Thread uuid/canonical/short-id/"
                               "summary-substring refs")
    p_thread.add_argument("--because", default=None, help="a short WHY, not an essay")
    p_thread.add_argument("--artifact", default=None,
                          help="a file:line/commit/decision proving the close")
    p_thread.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True,
                          help="preview only — ONLY takes effect with more than one ref "
                               "(the single-ref primitive never previews); default true")
    p_thread.add_argument("--actor", default=_CONSOLE_ACTOR,
                          help=f"who is closing this — defaults to {_CONSOLE_ACTOR!r}")
    p_thread.add_argument("--json", action="store_true", dest="as_json",
                          help="machine-readable: one compact JSON line")

    p_proposal = sub.add_parser("proposal", description=_d(
        "miners as last resort (decision ac892cd9) — propose/accept/reject over a "
        "Proposal, the same `proposal` MCP tool's own three actions"),
        epilog="example: osiris proposal accept --proposal-ref proposal:1234...\n"
               "example: osiris proposal reject --proposal-ref proposal:1234... "
               "--reason 'wrong shortlist'")
    p_proposal.add_argument("action", choices=("propose", "accept", "reject"))
    p_proposal.add_argument("--from-id", default=None,
                            help="propose only: the abstaining object's canonical or uuid")
    p_proposal.add_argument("--link-type", default=None,
                            help="propose only: must match the abstention's own namespace")
    p_proposal.add_argument("--candidate", default=None,
                            help="propose only: a JSON string, "
                                 "{\"kind\":\"link\",...} or {\"kind\":\"object\",...}")
    p_proposal.add_argument("--confidence", type=float, default=None,
                            help="propose only: capped at the DERIVED tier regardless")
    p_proposal.add_argument("--owner", default=None,
                            help="propose only: an active seat, its handle, or 'operator'")
    p_proposal.add_argument("--miner", default=None, help="propose only: the proposing miner")
    p_proposal.add_argument("--proposal-ref", default=None,
                            help="accept/reject: the Proposal's own canonical")
    p_proposal.add_argument("--reason", default=None,
                            help="reject only: mandatory, the miner reads it back")
    p_proposal.add_argument("--actor", default=_CONSOLE_ACTOR,
                            help=f"who is acting — defaults to {_CONSOLE_ACTOR!r}")
    p_proposal.add_argument("--json", action="store_true", dest="as_json",
                            help="machine-readable: one compact JSON line")

    p_rebind_seat = sub.add_parser(
        "rebind-seat", description=_d(
            "move a seat's ANCHOR cwd, preserving identity, lineage, attribution, and "
            "mail — the same orchestrator.mounts.rebind_seat the MCP tool wraps, exposed "
            "as the console door a human runs this from (jesus/chad's own self-"
            "reconciliation sequence, msg 6374)"),
        epilog="example: osiris rebind-seat Jesus /home/user/code/godel")
    p_rebind_seat.add_argument("seat", help="a claimed handle, a raw agent id, or an "
                               "unclaimed seat's own handle/canonical")
    p_rebind_seat.add_argument("new_cwd", help="the destination directory")
    p_rebind_seat.add_argument("--extract", action="store_true",
                               help="leave a SHARED cwd taking only this lineage's own "
                                    "transcripts, rather than moving the whole project")
    p_rebind_seat.add_argument("--because", default="",
                               help="why this seat is moving — the audit reason the MCP "
                                    "tool takes; a repair with no stated reason is exactly "
                                    "what this house forbids, so supply it")
    p_rebind_seat.add_argument("--force", action="store_true",
                               help="override rebind_seat's own refusals (it refuses loudly "
                                    "by default) — same flag, same semantics as the MCP tool")
    p_rebind_seat.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this rebind — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_correct_pin = sub.add_parser(
        "correct-pin-value", description=_d(
            "correct an EXISTING key in a seat's own `.osiris` pin — the same "
            "orchestrator.offices.correct_own_pin_value the MCP tool wraps, given an "
            "explicit target instead of an implicit mounted one (a terminal has no "
            "mounted identity to be self-scoped about)"),
        epilog="example: osiris correct-pin-value Jesus project Godel "
            "--because \"anchor moved, pin still named the old project\"")
    p_correct_pin.add_argument("seat", help="a claimed handle or a raw agent id — the "
                               "seat whose pin this corrects")
    p_correct_pin.add_argument("key", help="the already-declared pin key to rewrite")
    p_correct_pin.add_argument("value", help="the corrected value")
    p_correct_pin.add_argument("--because", required=True, dest="reason",
                               help="why this correction is being made — never optional, "
                                    "same rule as the MCP tool")

    p_heal_anchor = sub.add_parser(
        "heal-seat-anchor", description=_d(
            "assert THE ANCHOR INVARIANT (ruling 23771416) for one seat — anchor_cwd is "
            "identity, always <office_root>/<handle>, never wherever a session happened "
            "to be sitting. The same identity_heal.heal_seat_anchor_third_party the MCP "
            "tool wraps, given an explicit target (a terminal has no mounted identity to "
            "be self-scoped about)"),
        epilog="example: osiris heal-seat-anchor Jesus --because "
            "\"anchor invariant repair\" --apply")
    p_heal_anchor.add_argument("seat", help="a claimed handle or a raw agent id — the "
                               "seat whose anchor this heals")
    p_heal_anchor.add_argument("--because", required=True,
                               help="why this repair is being run — never optional, same "
                                    "rule as the MCP tool")
    p_heal_anchor.add_argument("--apply", action="store_true",
                               help="actually write — default is a dry-run report, same "
                                    "convention as every other repair verb in this house")
    p_heal_anchor.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this repair — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_transition = sub.add_parser(
        "transition-seat-project", description=_d(
            "move a seat's project binding from a fabricated handle-project to the "
            "real repo it already works in, in one composed act — the same "
            "transition.transition_seat_project the MCP tool wraps (the Jesus/Chad "
            "specimen's own hand-run sequence, now one call). Given an explicit "
            "target (a terminal has no mounted identity to be self-scoped about)"),
        epilog="example: osiris transition-seat-project Jesus --real-project Godel "
            "--because \"fabricated project, real repo already worked in\" --apply")
    p_transition.add_argument("seat", help="a claimed handle or a raw agent id — the "
                              "seat whose binding this transitions")
    p_transition.add_argument("--fabricated-project", default=None,
                              help="the project to transition away from — defaults to "
                                   "the seat's own handle (the specimen shape)")
    p_transition.add_argument("--real-project", default=None,
                              help="the project to transition onto — required only when "
                                   "the seat carries more than one other live works_in "
                                   "edge; otherwise auto-picked")
    p_transition.add_argument("--repos", default=None,
                              help="comma-separated charter repos to declare — defaults "
                                   "to [real-project]")
    p_transition.add_argument("--because", default="",
                              help="why this transition is being made — required with "
                                   "--apply, same rule as the MCP tool")
    p_transition.add_argument("--apply", action="store_true",
                              help="actually write — default is a dry-run plan, same "
                                   "convention as every other repair verb in this house")

    p_correct_agent_house = sub.add_parser(
        "correct-agent-house", description=_d(
            "heal an already-polluted agent's own project/seat_generation stamps — the "
            "same orchestrator.agents.correct_agent_house the MCP tool wraps, given an "
            "explicit target (a terminal has no mounted identity to be self-scoped "
            "about; correct-house's own self-scoped act has no console door for that "
            "reason)"),
        epilog="example: osiris correct-agent-house Jesus --project godel")
    p_correct_agent_house.add_argument("seat", help="a claimed handle or a raw agent id "
                                       "— the agent whose stamps this corrects")
    p_correct_agent_house.add_argument("--project", default=None,
                                       help="the corrected project stamp")
    p_correct_agent_house.add_argument("--seat-generation", type=int, default=None,
                                       dest="seat_generation",
                                       help="the corrected seat_generation stamp")
    p_correct_agent_house.add_argument("--actor", default=_CONSOLE_ACTOR,
                                       help=f"who is performing this correction — "
                                            f"defaults to {_CONSOLE_ACTOR!r}")

    p_reconcile_merge = sub.add_parser(
        "reconcile-merge", description=_d(
            "repair the estate a partial first fold left stranded on an ALREADY-MERGED "
            "dupe — the same orchestrator.merge.reconcile_merge the MCP tool wraps. "
            "Never re-performs the merge itself (that's `merge`'s job); unmerge-then-"
            "remerge is not a substitute"),
        epilog="example: osiris reconcile-merge agent:deadbeef agent:c0ffee")
    p_reconcile_merge.add_argument("dupe", help="the already-merged duplicate — type is "
                                   "read off its own form, same rule as merge/unmerge")
    p_reconcile_merge.add_argument("into", help="the surviving target")
    p_reconcile_merge.add_argument("--actor", default=_CONSOLE_ACTOR,
                                   help=f"who is performing this reconcile — defaults to "
                                        f"{_CONSOLE_ACTOR!r}")

    p_retire_agent = sub.add_parser(
        "retire-agent", description=_d(
            "third-party agent retirement — the same orchestrator.agents.retire_agent "
            "the MCP tool wraps, complementing the self-scoped `retire` (no target "
            "param, a raw terminal has no mounted session of its own to retire). "
            "ALWAYS releases the target's held seat and mount rows on success"),
        epilog="example: osiris retire-agent agent:deadbeef --because "
            "\"lineage superseded, stale test agent\"")
    p_retire_agent.add_argument("seat", help="a claimed handle or a raw agent id — the "
                                "agent to retire")
    p_retire_agent.add_argument("--because", required=True,
                                help="why this agent is being retired — never optional, "
                                     "same rule as the MCP tool")
    p_retire_agent.add_argument("--override-live", action="store_true",
                                dest="override_live",
                                help="retire even if the target reads LIVE (seen within "
                                     "15 min) — refused otherwise")
    p_retire_agent.add_argument("--actor", default=_CONSOLE_ACTOR,
                                help=f"who is performing this retirement — defaults to "
                                     f"{_CONSOLE_ACTOR!r}")

    p_fleet_reconcile = sub.add_parser(
        "fleet-reconcile", description=_d(
            "THE REAPER — buckets stale/anonymous agent mounts and acts on the fold-"
            "eligible ones, the same orchestrator.fleet_reconcile.reconcile_execute the "
            "MCP tool wraps. Dry run is the default: returns the plan, writes nothing"),
        epilog="example: osiris fleet-reconcile\n"
            "example, to actually write: osiris fleet-reconcile --execute")
    p_fleet_reconcile.add_argument("--execute", action="store_true",
                                   help="act on the plan rather than just report it — "
                                        "default is dry-run")
    p_fleet_reconcile.add_argument("--actor", default=_CONSOLE_ACTOR,
                                   help=f"who is performing this reconcile — defaults to "
                                        f"{_CONSOLE_ACTOR!r}")

    p_fleet_prune = sub.add_parser(
        "fleet-prune", description=_d(
            "THE MECHANICAL PRUNE (thread 07ca68ca) — dead_transcript (a mount row whose "
            "own job_dir is gone from disk) and unclaimed_body (a live OS body bound to "
            "its seat when tree_seat_hint resolves one), the same orchestrator.fleet_prune"
            ".prune_execute the MCP tool wraps. Dry run is the default: returns the plan, "
            "writes nothing. fleet-reconcile's own identity-folding buckets are untouched "
            "here — see that command instead"),
        epilog="example: osiris fleet-prune\n"
            "example, to actually write: osiris fleet-prune --execute")
    p_fleet_prune.add_argument("--execute", action="store_true",
                               help="act on the plan rather than just report it — "
                                    "default is dry-run")
    p_fleet_prune.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this prune — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_heal_transcript = sub.add_parser(
        "heal-seat-transcript", description=_d(
            "splice a seat's session, fragmented across multiple project slugs by a "
            "mid-session cwd move, back into ONE file at its own office slug — the same "
            "orchestrator.transcript_splice.heal_seat_transcript the MCP tool wraps. "
            "Never touches a Seat row, anchor_cwd, or any source transcript"),
        epilog="example: osiris heal-seat-transcript Jesus "
            "/path/to/fragment1.jsonl /path/to/fragment2.jsonl --apply --because "
            "\"mid-session cwd move split the transcript\"")
    p_heal_transcript.add_argument("seat", help="the seat whose office the spliced "
                                   "result lands at")
    p_heal_transcript.add_argument("source_paths", nargs="+",
                                   help="the original fragments, IN CHAIN ORDER (oldest "
                                        "first) — needs at least two")
    p_heal_transcript.add_argument("--because", default="",
                                   help="why this splice is being run — required to "
                                        "--apply, same rule as the MCP tool")
    p_heal_transcript.add_argument("--apply", action="store_true",
                                   help="actually write — default is a dry-run report, "
                                        "same convention as every other repair verb in "
                                        "this house")

    p_rematerialize = sub.add_parser(
        "rematerialize", description=_d(
            "reconstruct a session's transcript BYTE-FOR-BYTE from the soul store's "
            "soul_lines alone — the same SoulStore.rematerialize_to_"
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
        "new", description=(
            _d("Create a new independent seat and its workspace, in one command.") + "\n\n" +
            _d("You get: a directory to work in (~/code/<handle> unless you name one), "
               "a seat that owns it, and an identity office at "
               "~/.osiris/seats/<handle>/. Then `osiris launch <handle>` gives it a "
               "body.") + "\n\n" +
            _d("Use `new` for a seat that answers to nobody. Use `mint-seat` for a "
               "worker in a house you already run.")),
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
                            "pin — omit it and none is invented; it stays unset")
    p_new.add_argument("--house", default=None,
                       help="this seat's own house — omit it and none is invented; it "
                            "stays homeless (ruling 68fba2e4: homeless is a legal state)")
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


    p_attach_seat = sub.add_parser(
        "attach-seat", description=_d(
            "create a managed_by edge between two seats — the console-script door onto "
            "orchestrator.seats.attach_seat, the SAME function the attach_seat MCP tool "
            "wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris attach-seat Cassandra Thoth \"cross-house adoption\"")
    p_attach_seat.add_argument("worker", help="the seat gaining a manager")
    p_attach_seat.add_argument("manager", help="the seat becoming that manager")
    p_attach_seat.add_argument("evidence", help="why this edge is real")
    p_attach_seat.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this act — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_promote = sub.add_parser(
        "promote", description=_d(
            "mint a seat as manager over one or more workers, self-managed only — the "
            "console-script door onto orchestrator.seats.promote_seat, the SAME function "
            "the seat MCP tool's action='promote' branch wraps (operator 2026-09-07: "
            "'thoth cannot do it, it has to be self managed')"),
        epilog="example: osiris promote nebbercracker jenny chowder dustin "
              "--because \"nebbercracker leads monsterhouse now\"")
    p_promote.add_argument("target", help="the seat becoming the manager")
    p_promote.add_argument("workers", nargs="+", help="the seat(s) gaining this manager")
    p_promote.add_argument("--because", required=True, help="why this promotion is real")
    p_promote.add_argument("--actor", default=_CONSOLE_ACTOR,
                           help=f"who is performing this act — defaults to "
                                f"{_CONSOLE_ACTOR!r}")

    p_detach_seat = sub.add_parser(
        "detach-seat", description=_d(
            "invalidate a seat's active managed_by edge — the console-script door onto "
            "orchestrator.seats.detach_seat, the SAME function the detach_seat MCP tool "
            "wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris detach-seat Cassandra \"now self-managed\"")
    p_detach_seat.add_argument("seat", help="the seat losing its manager")
    p_detach_seat.add_argument("because", help="why this edge is being cut")
    p_detach_seat.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this act — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_vacate_seat = sub.add_parser(
        "vacate-seat", description=_d(
            "release a dead holder without retiring the seat itself — the console-"
            "script door onto orchestrator.trigger.vacate_dead_seat, the SAME function "
            "the vacate_seat MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris vacate-seat seat:e355913e \"holder confirmed dead\"")
    p_vacate_seat.add_argument("seat_id", help="the seat's own canonical id")
    p_vacate_seat.add_argument("because", help="why this holder is being released")
    p_vacate_seat.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this act — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_retire_seat = sub.add_parser(
        "retire-seat", description=_d(
            "mark a Seat permanently CLOSED, no successor, no merge target — the "
            "console-script door onto orchestrator.seats.retire_seat, the SAME function "
            "the retire_seat MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris retire-seat seat:e355913e --reason \"role is over\"")
    p_retire_seat.add_argument("seat_id", help="the seat's own canonical id")
    p_retire_seat.add_argument("--reason", default="", help="why this role is over")
    p_retire_seat.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this act — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_bind_seat_tree = sub.add_parser(
        "bind-seat-tree", description=_d(
            "point a seat's CODE checkout, distinct from its anchor office — the "
            "console-script door onto orchestrator.seats.bind_seat_tree, the SAME "
            "function the bind_seat_tree MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris bind-seat-tree seat:e355913e ~/code/osiris \"tree moved\"")
    p_bind_seat_tree.add_argument("seat_id", help="the seat's own canonical id")
    p_bind_seat_tree.add_argument("tree_cwd", help="the code checkout's directory")
    p_bind_seat_tree.add_argument("because", help="why this tree is moving")
    p_bind_seat_tree.add_argument("--actor", default=_CONSOLE_ACTOR,
                                  help=f"who is performing this act — defaults to "
                                       f"{_CONSOLE_ACTOR!r}")

    p_sweep_seat_disk = sub.add_parser(
        "sweep-seat-disk", description=_d(
            "sweep a retired seat's office and workspace off disk — the console-script "
            "door onto orchestrator.offices.sweep_retired_office + "
            "sweep_seat_workspace, the SAME two functions the sweep_seat_disk MCP tool "
            "wraps (wave 3, thread 5bf6447c). Dry-run by default"),
        epilog="example: osiris sweep-seat-disk OldHandle --apply --because retired")
    p_sweep_seat_disk.add_argument("handle", help="the retired seat's own handle")
    p_sweep_seat_disk.add_argument("--apply", action="store_true", dest="apply_",
                                   help="write; default is a dry-run report")
    p_sweep_seat_disk.add_argument("--because", default="", help="why this is being swept")

    p_rename_seat = sub.add_parser(
        "rename-seat", description=_d(
            "rename a seat's handle deliberately — the console-script door onto "
            "orchestrator.seats.rename_seat, the SAME function the rename_seat MCP "
            "tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris rename-seat seat:e355913e Till \"casing correction\"")
    p_rename_seat.add_argument("seat_id", help="the seat's own canonical id")
    p_rename_seat.add_argument("new_handle", help="the corrected handle")
    p_rename_seat.add_argument("because", help="why this rename is happening")
    p_rename_seat.add_argument("--actor", default=_CONSOLE_ACTOR,
                               help=f"who is performing this act — defaults to "
                                    f"{_CONSOLE_ACTOR!r}")

    p_set_seat_attended = sub.add_parser(
        "set-seat-attended", description=_d(
            "stamp a seat's real human-attendance signal — the console-script door onto "
            "orchestrator.seats.set_seat_attended, the SAME function the "
            "set_seat_attended MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris set-seat-attended seat:34f4e5fa true \"Thoth is human-driven\"")
    p_set_seat_attended.add_argument("seat_id", help="the seat's own canonical id")
    p_set_seat_attended.add_argument("attended", help="'true' or 'false'")
    p_set_seat_attended.add_argument("because", help="why this signal is being set")
    p_set_seat_attended.add_argument("--actor", default=_CONSOLE_ACTOR,
                                     help=f"who is performing this act — defaults to "
                                          f"{_CONSOLE_ACTOR!r}")

    p_reissue_office = sub.add_parser(
        "reissue-office", description=_d(
            "recompile a seat's managed office section on demand — the console-script "
            "door onto orchestrator.boot_compiler.reissue_office, the SAME function the "
            "reissue_office MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris reissue-office seat:e355913e \"manager changed\"")
    p_reissue_office.add_argument("seat_id", help="the seat's own canonical id")
    p_reissue_office.add_argument("because", help="why this office needs recompiling")
    p_reissue_office.add_argument("--adopt", action="store_true",
                                  help="one-time on-ramp for an office predating the "
                                       "compiler")
    p_reissue_office.add_argument("--actor", default=_CONSOLE_ACTOR,
                                  help=f"who is performing this act — defaults to "
                                       f"{_CONSOLE_ACTOR!r}")

    p_establish_office = sub.add_parser(
        "establish-office", description=_d(
            "the full office ceremony, one receipt — the console-script door onto "
            "orchestrator.offices.establish_office, the SAME function the "
            "establish_office MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris establish-office seat:e355913e")
    p_establish_office.add_argument("seat", help="a claimed handle, raw agent id, or "
                                    "unclaimed seat's own handle/canonical")
    p_establish_office.add_argument("--actor", default=_CONSOLE_ACTOR,
                                    help=f"who is performing this act — defaults to "
                                         f"{_CONSOLE_ACTOR!r}")

    p_resync_seat_house = sub.add_parser(
        "resync-seat-house", description=_d(
            "correct or unset a Seat's own house third-party — the console-script door "
            "onto orchestrator.seats.resync_seat_house_third_party, the SAME function "
            "the resync_seat_house MCP tool wraps (wave 3, thread 5bf6447c). Omit "
            "--house to UNSET a redundant house (never retire-assertion — thread "
            "dc1b5a20's own door note)"),
        epilog="example: osiris resync-seat-house seat:e355913e \"canon spelling\" "
              "--house ramstein"
              "\nexample, unsetting: osiris resync-seat-house seat:e355913e "
              "\"redundant with project\"")
    p_resync_seat_house.add_argument("seat_id", help="the seat's own canonical id")
    p_resync_seat_house.add_argument("reason", help="why this house is changing")
    p_resync_seat_house.add_argument("--house", default=None, dest="new_house",
                                     help="the corrected house — omit entirely to unset")
    p_resync_seat_house.add_argument("--actor", default=_CONSOLE_ACTOR,
                                     help=f"who is performing this act — defaults to "
                                          f"{_CONSOLE_ACTOR!r}")

    p_reconcile_seat_identity = sub.add_parser(
        "reconcile-seat-identity", description=_d(
            "third-party identity reconciliation for a seat that cannot correct itself "
            "— the console-script door onto orchestrator.identity_heal."
            "reconcile_seat_identity_third_party, the SAME function "
            "reconcile_seat_identity_third_party wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris reconcile-seat-identity seat:e355913e \"stale house row\"")
    p_reconcile_seat_identity.add_argument("seat_id", help="the seat's own canonical id")
    p_reconcile_seat_identity.add_argument("because", help="why this needs reconciling")
    p_reconcile_seat_identity.add_argument("--agent-id", default=None, dest="agent_id",
                                           help="omit to heal house alone")
    p_reconcile_seat_identity.add_argument("--actor", default=_CONSOLE_ACTOR,
                                           help=f"who is performing this act — defaults "
                                                f"to {_CONSOLE_ACTOR!r}")

    p_create_project = sub.add_parser(
        "create-project", description=_d(
            "declare a NEW SoftwareProject — the console-script door onto "
            "orchestrator.project_identity.create_project, the SAME function the "
            "create_project MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris create-project newthing \"standalone repo, no seat yet\"")
    p_create_project.add_argument("name", help="the new project's own name")
    p_create_project.add_argument("because", help="why this project is being declared")
    p_create_project.add_argument("--actor", default=_CONSOLE_ACTOR,
                                  help=f"who is performing this act — defaults to "
                                       f"{_CONSOLE_ACTOR!r}")

    p_rename_project = sub.add_parser(
        "rename-project", description=_d(
            "rename a SoftwareProject's own `name` property (canonical never moves) — "
            "the console-script door onto orchestrator.project_identity.rename_project, "
            "the SAME function the rename_project MCP tool wraps (wave 3, thread "
            "5bf6447c). Dry-run by default"),
        epilog="example: osiris rename-project oldname newname \"spelling fix\" --apply")
    p_rename_project.add_argument("project", help="the project's current name/canonical")
    p_rename_project.add_argument("new_name", help="the corrected name")
    p_rename_project.add_argument("because", help="why this rename is happening")
    p_rename_project.add_argument("--apply", action="store_true", dest="apply_",
                                  help="write; default is a dry-run report")
    p_rename_project.add_argument("--merge-into", action="store_true", dest="merge_into",
                                  help="lift the collision refusal when the new name "
                                       "already names an active project — folds into it")
    p_rename_project.add_argument("--actor", default=_CONSOLE_ACTOR,
                                  help=f"who is performing this act — defaults to "
                                       f"{_CONSOLE_ACTOR!r}")

    p_set_project_tag = sub.add_parser(
        "set-project-tag", description=_d(
            "declare a SoftwareProject's persisted window `[TAG]` override — the "
            "console-script door onto orchestrator.projects.set_project_window_tag, "
            "the SAME function the project(action='set_tag') MCP verb wraps. "
            "trigger.py's _house_tag/_window_name read this BEFORE ever deriving a tag "
            "from the house/project's own first two letters"),
        epilog="example: osiris set-project-tag monsterhouse MH \"operator's own code\"")
    p_set_project_tag.add_argument("project", help="the project's own name/canonical")
    p_set_project_tag.add_argument("tag", help="1-4 uppercase letters, exactly as wanted")
    p_set_project_tag.add_argument("because", help="why this tag is being declared")
    p_set_project_tag.add_argument("--actor", default=_CONSOLE_ACTOR,
                                   help=f"who is performing this act — defaults to "
                                        f"{_CONSOLE_ACTOR!r}")

    p_retire_project = sub.add_parser(
        "retire-project", description=_d(
            "retire a dead SoftwareProject stub — the console-script door onto "
            "orchestrator.projects.retire_project, the SAME function the "
            "retire_project MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris retire-project deadthing \"never went anywhere\"")
    p_retire_project.add_argument("project", help="the project's own name/canonical")
    p_retire_project.add_argument("because", help="why this project is being retired")
    p_retire_project.add_argument("--actor", default=_CONSOLE_ACTOR,
                                  help=f"who is performing this act — defaults to "
                                       f"{_CONSOLE_ACTOR!r}")

    p_fork_project = sub.add_parser(
        "fork-project", description=_d(
            "declare (or reverse) a fork relationship between two ALREADY-active "
            "SoftwareProjects — the console-script door onto "
            "orchestrator.project_identity.fork_project/unfork_project, the SAME "
            "functions the fork_project MCP tool wraps (wave 3, thread 5bf6447c)"),
        epilog="example: osiris fork-project redmonth ballgem \"new sibling project\""
              "\nexample, reversing: osiris fork-project redmonth ballgem \"mistake\" "
              "--direction unfork")
    p_fork_project.add_argument("project", help="the ancestor project")
    p_fork_project.add_argument("fork_into", help="the successor project")
    p_fork_project.add_argument("because", help="why this fork is happening")
    p_fork_project.add_argument("--direction", choices=["fork", "unfork"], default="fork",
                                help="'unfork' invalidates a live forked_from edge instead")
    p_fork_project.add_argument("--actor", default=_CONSOLE_ACTOR,
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
        return asyncio.run(cmd_smoke(chaos=args.chaos, as_json=args.as_json))
    if args.command == "boot-status":
        return asyncio.run(cmd_boot_status(as_json=args.as_json))
    if args.command == "lint":
        return asyncio.run(cmd_lint(
            check=args.check, project=args.project, as_json=args.as_json,
            stale_days=args.stale_days, limit=args.limit, offset=args.offset))
    if args.command == "audit":
        return asyncio.run(cmd_audit(args.name, as_json=args.as_json))
    if args.command == "seed":
        return asyncio.run(cmd_seed(compositions_only=args.compositions_only))
    if args.command == "launch":
        return asyncio.run(cmd_launch(args.handle, model=args.model, debug=args.debug))
    if args.command == "resume":
        return asyncio.run(cmd_resume(args.handle, model=args.model))
    if args.command == "stop":
        return asyncio.run(cmd_stop(args.handle, reason=args.reason, as_json=args.as_json))
    if args.command == "status":
        return asyncio.run(cmd_status(as_json=args.as_json))
    if args.command == "search":
        return asyncio.run(cmd_search(args.query, limit=args.limit, as_json=args.as_json))
    if args.command == "fleet":
        return asyncio.run(cmd_fleet(full=args.full, as_json=args.as_json))
    if args.command == "roster":
        return asyncio.run(cmd_roster(repo=args.repo, want_caveats=args.want_caveats,
                                      as_json=args.as_json))
    if args.command == "backlog":
        return asyncio.run(cmd_backlog(all_projects=args.all_projects, fleet=args.fleet,
                                       as_json=args.as_json))
    if args.command == "threads":
        return asyncio.run(cmd_threads(project=args.project, as_json=args.as_json))
    if args.command == "inbox":
        return asyncio.run(cmd_inbox(project=args.project, as_json=args.as_json))
    if args.command == "team":
        return asyncio.run(cmd_team(seat=args.seat, as_json=args.as_json))
    if args.command == "desk":
        return asyncio.run(cmd_desk(as_json=args.as_json))
    if args.command == "show":
        return asyncio.run(cmd_show(args.ref, as_json=args.as_json))
    if args.command == "migrate":
        return asyncio.run(cmd_migrate(check=args.check))
    if args.command == "deploy":
        return asyncio.run(cmd_deploy())
    if args.command == "merge":
        return asyncio.run(cmd_merge(args.dupe, args.into, args.evidence, actor=args.actor,
                                     force=args.force, because=args.because))
    if args.command == "unmerge":
        return asyncio.run(cmd_unmerge(args.dupe, args.because, actor=args.actor,
                                       execute=args.execute, as_json=args.as_json))
    if args.command == "retention":
        return asyncio.run(cmd_retention(args.table, days=args.days, execute=args.execute,
                                         batch_size=args.batch_size,
                                         as_json=args.as_json))
    if args.command == "fold-project":
        return asyncio.run(cmd_fold_project(args.dupe, args.into, args.evidence,
                                            actor=args.actor, force=args.force,
                                            because=args.because))
    if args.command == "charter-for":
        repos = [r.strip() for r in args.repos.split(",") if r.strip()]
        return asyncio.run(cmd_charter_for(args.seat, repos, args.because, actor=args.actor,
                                           ruling=args.ruling))
    if args.command == "settings":
        return asyncio.run(cmd_settings(
            args.action, key=args.key, value=args.value, because=args.because,
            ruling=args.ruling, scope_id=args.scope_id, actor=args.actor,
            as_json=args.as_json))
    if args.command == "amend-practice":
        return asyncio.run(cmd_amend_practice(args.ref, args.amendment, actor=args.actor))
    if args.command == "annotate-thread":
        return asyncio.run(cmd_annotate_thread(args.ref, args.note, actor=args.actor))
    if args.command == "amend-decision":
        return asyncio.run(cmd_amend_decision(args.ref, args.addendum, actor=args.actor))
    if args.command == "send":
        return asyncio.run(cmd_send(
            args.body, to=args.to, to_agent=args.to_agent, reply_to=args.reply_to,
            desk=args.desk, grade=args.grade, require_seat=args.require_seat,
            threads=args.threads, want_prior_art=args.want_prior_art,
            want_listener=args.want_listener, from_project=args.from_project,
            actor=args.actor, as_json=args.as_json))
    if args.command == "decide":
        return asyncio.run(cmd_decide(
            args.summary, kind=args.kind, rationale=args.rationale, repo=args.repo,
            grounds=args.grounds, protocol=args.protocol, supersedes=args.supersedes,
            resolves=args.resolves, obsoletes=args.obsoletes, confirms=args.confirms,
            refutes=args.refutes, implements=args.implements,
            rediscovers=args.rediscovers, bears_on=args.bears_on, narrows=args.narrows,
            cites=args.cites, ack_prior_art=args.ack_prior_art,
            unlinked_because=args.unlinked_because,
            operator_authorized=args.operator_authorized, actor=args.actor,
            as_json=args.as_json))
    if args.command == "thread":
        return asyncio.run(cmd_thread(
            args.ref, because=args.because, artifact=args.artifact,
            dry_run=args.dry_run, actor=args.actor, as_json=args.as_json))
    if args.command == "proposal":
        return asyncio.run(cmd_proposal(
            args.action, from_id=args.from_id, link_type=args.link_type,
            candidate=args.candidate, confidence=args.confidence, owner=args.owner,
            miner=args.miner, proposal_ref=args.proposal_ref, reason=args.reason,
            actor=args.actor, as_json=args.as_json))
    if args.command == "rebind-seat":
        return asyncio.run(cmd_rebind_seat(args.seat, args.new_cwd, actor=args.actor,
                                           extract=args.extract, because=args.because,
                                           force=args.force))
    if args.command == "correct-pin-value":
        return asyncio.run(cmd_correct_pin_value(args.seat, args.key, args.value,
                                                  args.reason))
    if args.command == "heal-seat-anchor":
        return asyncio.run(cmd_heal_seat_anchor(args.seat, because=args.because,
                                                apply=args.apply, actor=args.actor))
    if args.command == "transition-seat-project":
        transition_repos = args.repos.split(",") if args.repos else None
        return asyncio.run(cmd_transition_seat_project(
            args.seat, because=args.because, fabricated_project=args.fabricated_project,
            real_project=args.real_project, repos=transition_repos, apply=args.apply))
    if args.command == "correct-agent-house":
        return asyncio.run(cmd_correct_agent_house(
            args.seat, project=args.project, seat_generation=args.seat_generation,
            actor=args.actor))
    if args.command == "reconcile-merge":
        return asyncio.run(cmd_reconcile_merge(args.dupe, args.into, actor=args.actor))
    if args.command == "retire-agent":
        return asyncio.run(cmd_retire_agent(
            args.seat, args.because, override_live=args.override_live, actor=args.actor))
    if args.command == "fleet-reconcile":
        return asyncio.run(cmd_fleet_reconcile(execute=args.execute, actor=args.actor))
    if args.command == "fleet-prune":
        return asyncio.run(cmd_fleet_prune(execute=args.execute, actor=args.actor))
    if args.command == "heal-seat-transcript":
        return asyncio.run(cmd_heal_seat_transcript(
            args.seat, args.source_paths, apply=args.apply, because=args.because))
    if args.command == "rematerialize":
        return asyncio.run(cmd_rematerialize(args.anchor_sid, dest=args.dest,
                                             force=args.force))
    if args.command == "mint-seat":
        return asyncio.run(cmd_mint_seat(
            args.handle, manager=args.manager, project=args.project, house=args.house,
            model=args.model, actor=args.actor, adopt=args.adopt, force=args.force))
    if args.command == "new":
        return asyncio.run(cmd_new(
            args.handle, args.path, project=args.project, house=args.house,
            model=args.model, actor=args.actor))
    if args.command == "bootstrap":
        return asyncio.run(cmd_bootstrap(args.cwd, project=args.project, actor=args.actor))
    if args.command == "promote":
        return asyncio.run(cmd_promote(args.target, args.workers, args.because,
                                       actor=args.actor))
    if args.command == "attach-seat":
        return asyncio.run(cmd_attach_seat(args.worker, args.manager, args.evidence,
                                           actor=args.actor))
    if args.command == "detach-seat":
        return asyncio.run(cmd_detach_seat(args.seat, args.because, actor=args.actor))
    if args.command == "vacate-seat":
        return asyncio.run(cmd_vacate_seat(args.seat_id, args.because, actor=args.actor))
    if args.command == "retire-seat":
        return asyncio.run(cmd_retire_seat(args.seat_id, args.reason, actor=args.actor))
    if args.command == "bind-seat-tree":
        return asyncio.run(cmd_bind_seat_tree(args.seat_id, args.tree_cwd, args.because,
                                              actor=args.actor))
    if args.command == "sweep-seat-disk":
        return asyncio.run(cmd_sweep_seat_disk(args.handle, dry_run=not args.apply_,
                                               because=args.because))
    if args.command == "rename-seat":
        return asyncio.run(cmd_rename_seat(args.seat_id, args.new_handle, args.because,
                                           actor=args.actor))
    if args.command == "set-seat-attended":
        return asyncio.run(cmd_set_seat_attended(args.seat_id, args.attended, args.because,
                                                  actor=args.actor))
    if args.command == "reissue-office":
        return asyncio.run(cmd_reissue_office(args.seat_id, args.because, adopt=args.adopt,
                                              actor=args.actor))
    if args.command == "establish-office":
        return asyncio.run(cmd_establish_office(args.seat, actor=args.actor))
    if args.command == "resync-seat-house":
        return asyncio.run(cmd_resync_seat_house(args.seat_id, args.new_house, args.reason,
                                                  actor=args.actor))
    if args.command == "reconcile-seat-identity":
        return asyncio.run(cmd_reconcile_seat_identity(
            args.seat_id, args.because, agent_id=args.agent_id, actor=args.actor))
    if args.command == "create-project":
        return asyncio.run(cmd_create_project(args.name, args.because, actor=args.actor))
    if args.command == "rename-project":
        return asyncio.run(cmd_rename_project(
            args.project, args.new_name, args.because, dry_run=not args.apply_,
            merge_into=args.merge_into, actor=args.actor))
    if args.command == "set-project-tag":
        return asyncio.run(cmd_set_project_tag(
            args.project, args.tag, args.because, actor=args.actor))
    if args.command == "retire-project":
        return asyncio.run(cmd_retire_project(args.project, args.because, actor=args.actor))
    if args.command == "fork-project":
        return asyncio.run(cmd_fork_project(
            args.project, args.fork_into, args.because, direction=args.direction,
            actor=args.actor))
    return 2  # pragma: no cover - every real subparser choice is handled above; argparse
    # itself refuses anything not in `sub.choices`, so this is unreachable in practice


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
