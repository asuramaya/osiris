"""`osiris nas-first-offload`: the NAS's first offload, as one command to run when you are home.

The operator is usually remote, and an offload never goes over Tailscale (a full vault upload
through a tunnel is exactly what must not happen), so the NAS has had no successful offload for
as long as they have been away. This command is the whole job for the day they are home on the
LAN. It runs six steps in order, prints one plain line for each, and stops at the first one that
fails, naming what to do:

  1. the route to the NAS leaves through a local interface (never tailscale0), proved by the
     kernel's own route answer and the NAS answering on it
  2. the restic repository is there (created only if this target has never offloaded)
  3. the offload: the vault (WAL, base backups, dumps) is backed up
  4. a fresh snapshot of the vault exists in the repository
  5. the restore drill passes against that repository
  6. the recovery file is copied to the NAS

It takes the offload tick's own lock for the whole run, so the timers cannot start a second
restic beside it. It never reads, writes or moves the plaintext copy of the vault key (that waits
until it is cleared); the recovery file it copies is the already-wrapped enrollment file the
offload tick copies anyway.

Every outside effect is a function passed in (`Deps`), so the tests run the whole job against
fakes: no restic, no network, no NAS."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

TOTAL = 6
FRESH_SNAPSHOT = timedelta(minutes=60)


@dataclass
class Deps:
    """The outside world. The defaults are the real functions; a test replaces any of them."""
    presence: Callable[[dict[str, Any]], dict[str, Any]]
    # (repo, password) -> ("ok", n_snapshots) | ("missing", "") | ("busy", why) | ("error", why)
    repo_state: Callable[[str, bytes], tuple[str, Any]]
    init_repo: Callable[[str, bytes], str | None]
    # (repo, password, source) -> (failure or None, note or None)
    offload: Callable[[str, bytes, Path], tuple[str | None, str | None]]
    # (repo, password) -> the newest snapshot {id, time, paths} or None
    latest_snapshot: Callable[[str, bytes], dict[str, Any] | None]
    drill: Callable[[str], str | None]
    copy_recovery: Callable[[dict[str, Any]], dict[str, Any] | None]
    ever_offloaded: Callable[[str], bool]
    record_offload: Callable[[str, str, str | None], None]
    record_drill: Callable[[str, str | None, str | None], None]
    hold_lock: Callable[[], Any]
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))


def _env(repo: str, password: bytes) -> dict[str, str]:
    return {**os.environ, "RESTIC_REPOSITORY": repo, "RESTIC_PASSWORD": password.decode()}


def _repo_state(repo: str, password: bytes) -> tuple[str, Any]:
    from src.orchestrator import restic_locks
    from src.orchestrator.network_presence import is_unreachable_error
    from src.orchestrator.offload_runner import _one_line, repository_is_missing

    env = _env(repo, password)
    try:
        outcome = restic_locks.clear_stale_locks(env, blocking_only_exclusive=True)
        if outcome.busy:
            return "busy", outcome.busy
        probe = restic_locks.run_restic(["snapshots", "--json"], env, timeout=60, text=False)
    except Exception as exc:  # noqa: BLE001 - reported as a step failure, never a traceback
        return "error", f"{type(exc).__name__}: {exc}"
    if probe.returncode == 0:
        try:
            return "ok", len(json.loads(probe.stdout or b"[]"))
        except ValueError:
            return "ok", 0
    detail = (probe.stdout + b"\n" + probe.stderr).decode(errors="replace")
    if restic_locks.is_lock_failure(detail, probe.returncode):
        return "busy", restic_locks.holder_line(detail) or _one_line(probe.stderr)
    if repository_is_missing(detail, probe.returncode):
        return "missing", ""
    reason = _one_line(probe.stderr)
    return ("unreachable" if is_unreachable_error(reason) else "error"), reason


def _init_repo(repo: str, password: bytes) -> str | None:
    from src.orchestrator import restic_locks

    done = restic_locks.run_restic(["init"], _env(repo, password), timeout=120, text=False)
    return None if done.returncode == 0 else done.stderr.decode(errors="replace").strip()[-300:]


def _offload(repo: str, password: bytes, source: Path) -> tuple[str | None, str | None]:
    from src.orchestrator.offload_runner import _run_restic_backup

    notes: list[str] = []
    fail = _run_restic_backup(repository=repo, password=password, source=source,
                              init_if_needed=False, notes=notes)
    return fail, (notes[0] if notes else None)


def parse_time(text: object) -> datetime | None:
    """A restic snapshot time as an aware UTC datetime. restic writes the box's local offset
    ("...-06:00"), sometimes 'Z', and nanoseconds; none of that may change which snapshot is the
    newest, so times are compared as instants, never as strings."""
    import re

    raw = str(text or "").strip()
    if not raw:
        return None
    raw = re.sub(r"(\.\d{6})\d+", r"\1", raw)  # nanoseconds: keep microseconds
    try:
        when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return when.astimezone(UTC) if when.tzinfo else when.replace(tzinfo=UTC)


def _latest_snapshot(repo: str, password: bytes) -> dict[str, Any] | None:
    from src.orchestrator import restic_locks

    done = restic_locks.run_restic(["snapshots", "--json"], _env(repo, password),
                                   timeout=120, text=False)
    if done.returncode != 0:
        return None
    try:
        rows = json.loads(done.stdout or b"[]")
    except ValueError:
        return None
    dated: list[tuple[datetime, dict[str, Any]]] = []
    for r in rows:
        when_r = parse_time(r.get("time")) if isinstance(r, dict) else None
        if when_r is not None and isinstance(r, dict):
            dated.append((when_r, r))
    if not dated:
        return None
    when, newest = max(dated, key=lambda pair: pair[0])
    return {"id": str(newest.get("short_id") or str(newest.get("id", ""))[:8]),
            "time": when.isoformat(), "paths": list(newest.get("paths") or [])}


def _ever_offloaded(name: str) -> bool:
    from src.orchestrator.offload_runner import _load_receipts

    receipts, untrusted = _load_receipts()
    return untrusted or bool((receipts.get(name) or {}).get("last_successful_offload"))


def _record_offload(name: str, now: str, note: str | None) -> None:
    from src.orchestrator.offload_runner import _log_attempt, _write_receipt

    _log_attempt(name, "ok_with_note" if note else "ok", now, duration_s=0.0, detail=note)
    _write_receipt(name, {"last_successful_offload": now, "last_attempt_at": now,
                          "last_error": None, "last_skip_reason": None, "skipped_since": None,
                          "stale_notified_clock": None, "last_note": note})


def _record_drill(url: str, fail: str | None, note: str | None) -> None:
    from src.orchestrator.soul_key import record_restore_drill

    record_restore_drill(url, ok=fail is None, error=fail, note=note)


def real_deps() -> Deps:
    from src.orchestrator import recovery_copies
    from src.orchestrator.network_presence import lan_presence
    from src.orchestrator.offload_runner import hold_tick_lock
    from src.orchestrator.scheduled_drill import _real_run_drill

    return Deps(
        presence=lan_presence, repo_state=_repo_state, init_repo=_init_repo, offload=_offload,
        latest_snapshot=_latest_snapshot, drill=_real_run_drill,
        copy_recovery=lambda target: recovery_copies.copy_to_target(target),
        ever_offloaded=_ever_offloaded, record_offload=_record_offload,
        record_drill=_record_drill, hold_lock=hold_tick_lock)


def pick_target(
    targets: list[dict[str, Any]], name: str | None,
) -> tuple[dict[str, Any] | None, str]:
    """The enabled restic NAS target to use, or (None, why not)."""
    from src.orchestrator.network_presence import sftp_host

    nas = [t for t in targets if isinstance(t, dict) and t.get("enabled")
           and t.get("kind") == "restic" and sftp_host(str(t.get("path_or_url") or ""))]
    if name is not None:
        nas = [t for t in nas if t.get("name") == name]
    if not nas:
        return None, ("no enabled NAS target is configured" if name is None
                      else f"no enabled NAS target is named {name!r}")
    if len(nas) > 1:
        names = ", ".join(str(t.get("name")) for t in nas)
        return None, f"more than one NAS target is enabled ({names}); name one with --name"
    return nas[0], ""


async def run(
    target: dict[str, Any], password: bytes, vault: Path, deps: Deps, *,
    say: Callable[[str], None] = print, check_only: bool = False,
) -> int:
    """The six steps against one target. 0 when every step passed (or, with `check_only`, when the
    two read-only steps did), 1 at the first that did not."""
    name = str(target.get("name", "<unnamed>"))
    repo = str(target.get("path_or_url") or "")

    def step(n: int, text: str) -> None:
        say(f"[{n}/{TOTAL}] {text}")

    def stop(n: int, why: str, what_next: str) -> int:
        say(f"[{n}/{TOTAL}] STOPPED: {why}")
        say(f"        next: {what_next}")
        return 1

    # 1. HOME, ON THE LAN ROUTE
    seen = await asyncio.to_thread(deps.presence, target)
    if not seen.get("present"):
        return stop(1, f"not on the home network ({seen.get('reason') or 'route unknown'}).",
                    "run this again when you are home on the LAN, not over Tailscale.")
    if not seen.get("dev"):
        return stop(1, "could not confirm which interface the route to the NAS leaves through; "
                       "not going without that proof.",
                    "check `ip route get` for the NAS address, then run this again.")
    step(1, f"home network: the route to the NAS leaves through {seen['dev']} and the NAS "
            "answers there.")

    with deps.hold_lock() as locked:
        if not locked:
            return stop(1, "an offload or restore test is running right now.",
                        "wait a few minutes (the timers take about 15 minutes) and run this again.")
        # 2. THE REPOSITORY
        state, info = await asyncio.to_thread(deps.repo_state, repo, password)
        if state == "ok":
            step(2, f"the repository is there ({info} snapshot(s) so far).")
        elif state == "missing":
            if await asyncio.to_thread(deps.ever_offloaded, name):
                return stop(2, "the repository looks missing, but this target has offloaded "
                               "before; not creating a new one over it.",
                            "check the NAS pool is mounted and the path is right, then run "
                            "this again.")
            if check_only:
                step(2, "the repository does not exist yet; this run would create it.")
            else:
                made = await asyncio.to_thread(deps.init_repo, repo, password)
                if made:
                    return stop(2, f"could not create the repository ({made}).",
                                "check the NAS path and permissions, then run this again.")
                step(2, "the repository did not exist and was created.")
        elif state == "busy":
            return stop(2, f"the repository is in use ({info}).",
                        "wait for that to finish, then run this again.")
        elif state == "unreachable":
            return stop(2, f"the NAS did not answer ({info}).",
                        "check the NAS is on and reachable from this network, then run this "
                        "again.")
        else:
            return stop(2, f"could not read the repository ({info}).",
                        "fix that (a wrong password and a dropped session read the same), "
                        "then run this again.")
        if check_only:
            say("check only: steps 1 and 2 passed; nothing was uploaded.")
            say(f"to do the offload, run: osiris nas-first-offload --name {name}")
            return 0

        # 3. THE OFFLOAD
        step(3, f"offloading {vault} (WAL, base backups, dumps); the first run can take a long "
                "time, and restic skips what the NAS already holds.")
        fail, note = await asyncio.to_thread(deps.offload, repo, password, vault)
        if fail:
            return stop(3, f"the offload did not finish ({fail}).",
                        "run this again: restic resumes from what it already copied.")
        deps.record_offload(name, deps.now().isoformat(), note)
        step(3, "the offload finished" + (f" ({note})." if note else "."))

        # 4. A FRESH SNAPSHOT
        snap = await asyncio.to_thread(deps.latest_snapshot, repo, password)
        if snap is None:
            return stop(4, "the repository shows no snapshot after the offload.",
                        "do not trust this backup; run this again and read restic's own message.")
        taken = parse_time(snap["time"])
        if taken is None:
            return stop(4, f"the newest snapshot ({snap['id']}) has no readable time.",
                        "run this again; if it repeats, read `restic snapshots` by hand.")
        if deps.now() - taken > FRESH_SNAPSHOT:
            return stop(4, f"the newest snapshot ({snap['id']}) is from "
                           f"{taken:%Y-%m-%d %H:%M}, not from this run.",
                        "run this again; if it repeats, the backup is not writing a snapshot.")
        step(4, f"a fresh snapshot exists: {snap['id']} at {taken:%Y-%m-%d %H:%M} UTC.")

        # 5. THE RESTORE DRILL
        step(5, "restore test: checking the repository and restoring samples (a few minutes).")
        failure = await asyncio.to_thread(deps.drill, repo)
        deps.record_drill(repo, failure, None)
        if failure:
            return stop(5, f"the restore test failed ({failure}).",
                        "the backup is uploaded but NOT proved restorable; do not rely on it "
                        "until this passes.")
        step(5, "the restore test passed.")

        # 6. THE RECOVERY FILE, OFF THE BOX
        copied = await asyncio.to_thread(deps.copy_recovery, target)
        if copied is None:
            step(6, "the recovery file on the NAS is already current.")
        elif copied.get("ok"):
            step(6, "the recovery file was copied to the NAS.")
        else:
            return stop(6, f"the recovery file was not copied ({copied.get('error')}).",
                        "run this again; the offload and the restore test already passed.")
    say("done: the NAS holds a restorable backup. The timers take it from here.")
    return 0


async def command(
    *, name: str | None = None, check_only: bool = False, pool: Any = None,
    deps: Deps | None = None, say: Callable[[str], None] = print,
) -> int:
    """The CLI body: find the NAS target, then `run` it. Plain lines to stdout, plain reasons for a
    refusal, exit 0 or 1."""
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password

    settings = await get_backup_settings(pool)
    target, why = pick_target(settings.get("offload_targets") or [], name)
    if target is None:
        say(f"nas-first-offload: {why}.")
        return 1
    try:
        password = get_restic_password()
    except ResticPasswordMissing as exc:
        say(f"nas-first-offload: {exc}")
        return 1
    vault = Path(os.environ.get("OSIRIS_VAULT", str(Path.home() / "osiris-vault")))
    with contextlib.suppress(Exception):
        say(f"NAS first offload to {target.get('name')} ({target.get('path_or_url')}):")
    return await run(target, password, vault, deps or real_deps(), say=say,
                     check_only=check_only)
