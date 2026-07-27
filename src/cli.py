"""osiris — the operator's console-script (task #69, ruling 45b074bf, thread 16a0c76b: "no
bash runes; the user never debugs the machinery"). Five thin subcommands, each wrapping an
EXISTING verb rather than re-deriving it:

  osiris attach <handle>            resolve handle -> a live PTY session, hand off to
                                     src.manager.attach (replaces the raw
                                     `.venv/bin/python -m src.manager.attach "[OS] imhotep"`
                                     the operator was handed before this build)
  osiris smoke                      the same probe src.orchestrator.smoke runs for the fleet
  osiris seed [--compositions-only] src.init's seeder (task #63's own deploy-step flag)
  osiris launch <handle> [--model]  body a seat via the manager daemon DIRECTLY — never
                                     trigger.py's launch_seat(), which is explicitly a
                                     seat-to-seat verb ("THE OPERATOR NEVER CALLS THIS"); a
                                     human at this CLI is a different trust boundary, the
                                     same one src.manager.attach.py already stands in for
  osiris fleet [--full]             the same fleet() the MCP tool answers, called over the
                                     wire (never a second implementation of what it computes)
  osiris deploy                     the deploy ritual as one verb (thread e51a841c): refuse
                                     on a dirty tracked src/ tree (a live near-miss shipped a
                                     half-written edit this way), restart osiris-mcp/worker/
                                     console, run smoke, and name any un-run seeder/migration
                                     step by comparison instead of assuming one happened

CANONICAL ENV RESOLUTION (the actual root-fix, 3e96c10e's cousin): every DB-backed command
applies src.config.dev_env.apply_dev_fallback() first — a bare invocation must target the
SAME dev instance the systemd user units already inline, never silently fall to Settings'
prod-shaped 5432/6379 default. `attach`/`launch`'s manager-socket calls and `smoke`/`fleet`'s
MCP round-trip need no such fallback (neither touches Postgres directly); only `seed` and
`launch`'s own seat-facts lookup + honesty check do.

Every error is honest and names the next step — no raw traceback reaches the operator's
terminal for a condition this module can anticipate (a dark daemon, an unreachable database,
an ambiguous or unknown handle)."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import asyncpg

from src.manager.client import default_socket_path, manager_call

ManagerCall = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


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

async def _run_smoke_probes() -> list[str]:
    """The two probes, composed — shared by `cmd_smoke` and `cmd_deploy` so neither
    re-derives it. Returns the flat failure list (empty = all green)."""
    import httpx

    from src.config.settings import get_settings
    from src.orchestrator.smoke import call_mcp_smoke, smoke_chrome, summarize_failures

    settings = get_settings()
    url = await _mcp_url()

    async def local_chrome() -> dict[str, str]:
        async with httpx.AsyncClient(
            base_url=settings.osiris_console_base_url, timeout=5.0,
        ) as client:
            return await smoke_chrome(client)

    chrome, mcp_result = await asyncio.gather(local_chrome(), call_mcp_smoke(url))
    return summarize_failures(chrome, mcp_result)


async def cmd_smoke() -> int:
    fails = await _run_smoke_probes()
    if not fails:
        print("smoke: all green (8 chrome routes + the live mcp pool)")
        return 0
    print("SMOKE FAILURES:")
    for f in fails:
        print(" -", f)
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
            pool = await create_pool(settings.database_url, min_size=1, max_size=4)
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


async def cmd_launch(
    handle: str, *, model: str | None, pool: asyncpg.Pool | None = None,
    manager: ManagerCall = _default_manager, wake_default: str | None = None,
) -> int:
    """Bodies a seat via the manager daemon DIRECTLY (pty_spawn), never trigger.py's
    launch_seat(): that verb is explicitly seat-to-seat only ("THE OPERATOR NEVER CALLS
    THIS... an override a caller can assert in an argument is an override that can be forged;
    the operator's hand stays out-of-band") — a human driving this CLI already IS the
    out-of-band hand, the same trust boundary src.manager.attach.py stands in for. Reports
    the model it actually confirms mounted, honestly and within a bounded wait — never a bare
    'launched: true'. `pool`/`manager`/`wake_default` are injectable (mirrors launch_seat's
    own test seam) — production callers (main()) leave them at their real defaults."""
    from src.orchestrator.seats import seat_facts, seats_by_handle

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        wake_default = settings.osiris_wake_model
        try:
            pool = await create_pool(settings.database_url, min_size=1, max_size=2)
        except Exception as exc:  # noqa: BLE001
            print(f"osiris launch: could not reach postgres at {settings.database_url} — "
                  f"{exc}.", file=sys.stderr)
            return 1
    try:
        seat_ids = await seats_by_handle(pool, handle)
        if not seat_ids:
            print(f"osiris launch: no living Seat holds handle {handle!r}.", file=sys.stderr)
            return 1
        if len(seat_ids) > 1:
            print(f"osiris launch: {handle!r} is ambiguous — {len(seat_ids)} seats share it: "
                  f"{seat_ids}. Use a more specific handle.", file=sys.stderr)
            return 1
        facts = await seat_facts(pool, seat_ids[0])
        if not facts["handle"]:
            print(f"osiris launch: {seat_ids[0]} carries no handle assertion — a body cannot "
                  "be named for a nameless seat.", file=sys.stderr)
            return 1
        if not facts["anchor_cwd"]:
            print(f"osiris launch: {facts['handle']} ({seat_ids[0]}) has no anchor_cwd — "
                  "establish_office first; a body needs a room to be born in.", file=sys.stderr)
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
        anchor = str(Path.home() / ".claude" / "jobs" / seat_ids[0].replace(":", "-"))
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
    finally:
        if owns_pool:
            await pool.close()


# --- fleet -----------------------------------------------------------------------------------

async def cmd_fleet(*, full: bool) -> int:
    import json

    from src.orchestrator.mcp_client import call_mcp_tool

    url = await _mcp_url()
    result = await call_mcp_tool(url, "fleet", {"full": full})
    if isinstance(result, str):
        print(f"osiris fleet: {result} — is osiris-mcp running? "
              "(systemctl --user status osiris-mcp)", file=sys.stderr)
        return 1
    tree = result.get("tree")
    if isinstance(tree, str):
        print(tree)
    else:
        print(json.dumps(result, indent=2, default=str))
    return 0


# --- deploy ----------------------------------------------------------------------------------

DEPLOY_UNITS = ("osiris-mcp", "osiris-worker", "osiris-console")

GitStatus = Callable[[Path], list[tuple[str, str]]]
RestartServices = Callable[[list[str]], Awaitable[tuple[int, str]]]


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


def _alembic_head(repo_root: Path) -> str | None:
    """The latest migration's own revision id, read off the version files on disk — no DB
    connection needed for this half of the comparison. None (never a raised CommandError)
    when `repo_root` carries no alembic.ini/alembic/ at all — a repo_root that isn't this
    project's own checkout shape is a different problem than a migration gap, and
    alembic_gap_note treats None as 'could not be determined', never as a false mismatch."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    from alembic.util.exc import CommandError

    if not (repo_root / "alembic.ini").is_file():
        return None
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    try:
        return ScriptDirectory.from_config(cfg).get_current_head()
    except CommandError:
        return None


def composition_gap_note(db_count: int, expected: int) -> str | None:
    if db_count >= expected:
        return None
    return (f"compositions: DB has {db_count}, DEFAULT_COMPOSITIONS defines {expected} — "
           "run `osiris seed` (or `osiris seed --compositions-only`).")


def alembic_gap_note(current: str | None, head: str | None) -> str | None:
    if head is None or current == head:
        return None
    return (f"alembic: DB is at revision {current!r}, the latest migration is {head!r} — "
           "run `alembic upgrade head`.")


async def _seeder_migration_gaps(pool: asyncpg.Pool, repo_root: Path) -> list[str]:
    from src.orchestrator.compositions import DEFAULT_COMPOSITIONS

    db_count = await pool.fetchval("SELECT count(*) FROM compositions")
    current = await pool.fetchval("SELECT version_num FROM alembic_version")
    head = _alembic_head(repo_root)
    notes = [composition_gap_note(db_count, len(DEFAULT_COMPOSITIONS)),
             alembic_gap_note(current, head)]
    return [n for n in notes if n is not None]


async def cmd_deploy(
    *, repo_root: Path | None = None, git_status: GitStatus = _real_git_status,
    restart: RestartServices = _real_restart_services, pool: asyncpg.Pool | None = None,
) -> int:
    """The deploy ritual as one verb (thread e51a841c): a live near-miss held batch 3 because
    src/orchestrator/handshake.py carried another agent's uncommitted WIP and the three
    services import straight from the working tree — only a by-hand `git status` caught it
    before a restart would have shipped a half-written identity edit. Replaces that by-hand
    protocol: (1) refuse on a dirty tracked src/ tree, naming the files (never guesses whose
    WIP it is — check project mail for a collision-watch broadcast instead of trusting a
    fragile heuristic); (2) restart osiris-mcp/worker/console; (3) run smoke, per-surface;
    (4) name any un-run seeder/migration step by comparison, never by assumption. Also names
    (informationally, never gating) any dirty COMMIT-DEPLOYED script — a oneshot timer unit
    reads straight off disk, so nothing here can hold it back (msg 1481)."""
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

    rc, out = await restart(list(DEPLOY_UNITS))
    if rc != 0:
        print(f"osiris deploy: restart failed (exit {rc}): {out}", file=sys.stderr)
        return 1
    print(f"osiris deploy: restarted {', '.join(DEPLOY_UNITS)}")

    fails = await _run_smoke_probes()
    if fails:
        print("SMOKE FAILURES:")
        for f in fails:
            print(" -", f)
    else:
        print("smoke: all green")

    owns_pool = pool is None
    if pool is None:
        from src.config.dev_env import apply_dev_fallback
        from src.config.settings import get_settings
        from src.db.pool import create_pool

        apply_dev_fallback()
        settings = get_settings()
        try:
            pool = await create_pool(settings.database_url, min_size=1, max_size=2)
        except Exception as exc:  # noqa: BLE001
            print(f"osiris deploy: could not reach postgres to check seeder/migration state "
                  f"— {exc}. Restart + smoke above still stand.", file=sys.stderr)
            return 1 if fails else 0
    try:
        gaps = await _seeder_migration_gaps(pool, root)
    finally:
        if owns_pool:
            await pool.close()
    if gaps:
        print("UN-RUN STEPS:")
        for g in gaps:
            print(" -", g)
    else:
        print("seeder/migrations: up to date")

    return 1 if fails else 0


# --- argv dispatch -----------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="osiris", description="The osiris console-script — no "
                                "bash runes; every command names its own next step on failure.")
    sub = p.add_subparsers(dest="command", required=True)

    p_attach = sub.add_parser("attach", help="attach to a live seat's PTY session")
    p_attach.add_argument("handle")

    sub.add_parser("smoke", help="the same deploy-time liveness probe the fleet runs")

    p_seed = sub.add_parser("seed", help="seed default compositions (and rooms)")
    p_seed.add_argument("--compositions-only", action="store_true",
                        help="seed + room DEFAULT_COMPOSITIONS only; skip the canon ingest")

    p_launch = sub.add_parser("launch", help="body a seat (spawn its claude process)")
    p_launch.add_argument("handle")
    p_launch.add_argument("--model", default=None)

    p_fleet = sub.add_parser("fleet", help="the fleet roster, grouped by project")
    p_fleet.add_argument("--full", action="store_true")

    sub.add_parser("deploy", help="the deploy ritual: dirty-guard, restart, smoke, "
                   "un-run-step report")

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "attach":
        return asyncio.run(cmd_attach(args.handle))
    if args.command == "smoke":
        return asyncio.run(cmd_smoke())
    if args.command == "seed":
        return asyncio.run(cmd_seed(compositions_only=args.compositions_only))
    if args.command == "launch":
        return asyncio.run(cmd_launch(args.handle, model=args.model))
    if args.command == "fleet":
        return asyncio.run(cmd_fleet(full=args.full))
    if args.command == "deploy":
        return asyncio.run(cmd_deploy())
    return 2  # pragma: no cover - argparse's own `required=True` makes this unreachable


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
