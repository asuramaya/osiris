"""Stale restic repository locks, cleared by the tooling and never by hand.

A restic run killed mid-flight (a timeout, a stopped backup, a dropped link) leaves its lock in
the repository, and the next `restic check` then fails with "repo already locked" until someone
removes it. That is how the off-box restore test failed on every drill against the NAS since a
long backup was stopped. This module lists the locks, decides which are stale by restic's own
rule (older than 30 minutes, or taken on this machine by a process that no longer exists), runs
a plain `restic unlock` (which only ever removes stale locks, never `--remove-all`) when every
lock is stale, and otherwise says plainly who holds the repository instead of dumping stderr.

Every call is bounded the way the offload runner's `restic snapshots` probe is. Nothing here
runs unless it is handed a repository environment; tests drive it with a fake `restic`."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

STALE_AFTER = timedelta(minutes=30)
CALL_TIMEOUT_SECONDS = 30  # the same bound the offload runner gives `restic snapshots`

Runner = Callable[[list[str], dict[str, str]], "subprocess.CompletedProcess[str]"]


@dataclass(frozen=True)
class Lock:
    id: str
    time: datetime | None
    hostname: str
    pid: int | None
    exclusive: bool


@dataclass(frozen=True)
class LockOutcome:
    """What `clear_stale_locks` found and did. `busy` is set when a live lock remains (or the
    lock listing itself failed); `note` is a sentence for the drill receipt when anything was
    cleared."""

    cleared: tuple[Lock, ...] = ()
    busy: str | None = None
    note: str | None = None


def _run(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["restic", *args], env=env, capture_output=True, text=True,
                          timeout=CALL_TIMEOUT_SECONDS, check=False)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # it exists, it is just not ours
    return True


def _parse_time(raw: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def read_locks(env: dict[str, str], restic: Runner = _run) -> list[Lock] | str:
    """Every lock in the repository, or a one-line reason the listing could not be made."""
    listed = restic(["list", "locks", "--quiet"], env)
    if listed.returncode != 0:
        return _short(listed, "could not list the repository's locks")
    locks: list[Lock] = []
    for lock_id in listed.stdout.split():
        got = restic(["cat", "lock", lock_id], env)
        if got.returncode != 0:
            return _short(got, f"could not read lock {lock_id[:8]}")
        try:
            body = json.loads(got.stdout)
        except ValueError:
            return f"could not read lock {lock_id[:8]}: not valid JSON"
        pid = body.get("pid")
        locks.append(Lock(
            id=lock_id, time=_parse_time(body.get("time")),
            hostname=str(body.get("hostname") or "?"),
            pid=int(pid) if isinstance(pid, int) else None,
            exclusive=bool(body.get("exclusive"))))
    return locks


def _short(proc: subprocess.CompletedProcess[str], what: str) -> str:
    detail = (proc.stderr or proc.stdout or "").strip().splitlines()
    return f"{what}: {detail[-1][:200]}" if detail else what


def is_stale(lock: Lock, *, now: datetime, local_host: str,
             pid_alive: Callable[[int], bool] = _pid_alive) -> bool:
    """Restic's own rule: older than 30 minutes (a live restic refreshes its lock every few
    minutes, so an old one is a dead one, and a recycled pid must not keep it alive), or taken on
    this machine by a process that is gone. A lock with an unreadable time is never called stale
    by age alone."""
    if lock.time is not None and now - lock.time > STALE_AFTER:
        return True
    return lock.hostname == local_host and lock.pid is not None and not pid_alive(lock.pid)


def describe(lock: Lock) -> str:
    since = lock.time.strftime("%Y-%m-%d %H:%M UTC") if lock.time else "an unknown time"
    return f"lock by {lock.hostname}/{lock.pid if lock.pid is not None else '?'} since {since}"


def clear_stale_locks(
    env: dict[str, str], *, restic: Runner = _run, now: datetime | None = None,
    local_host: str | None = None, pid_alive: Callable[[int], bool] = _pid_alive,
    blocking_only_exclusive: bool = False,
) -> LockOutcome:
    """Removes the repository's stale locks when every lock is stale, and reports a live one.
    `blocking_only_exclusive` is for a backup, which coexists with other shared locks and is
    stopped only by an exclusive one."""
    now = now or datetime.now(UTC)
    local_host = local_host or socket.gethostname()
    try:
        locks = read_locks(env, restic)
    except (OSError, subprocess.TimeoutExpired) as exc:
        locks = f"{type(exc).__name__}: {exc}"
    if isinstance(locks, str):
        # The listing failed (a wrong password, an unreachable repository): that is the next
        # restic call's own error to report, not a lock problem to pre-empt it with.
        return LockOutcome()
    if not locks:
        return LockOutcome()
    stale = [lock for lock in locks if is_stale(lock, now=now, local_host=local_host,
                                                pid_alive=pid_alive)]
    live = [lock for lock in locks if lock not in stale]
    blocking = [lock for lock in live if lock.exclusive or not blocking_only_exclusive]
    if blocking:
        first = min(blocking, key=lambda lock: lock.time or now)
        more = f" (+{len(blocking) - 1} more)" if len(blocking) > 1 else ""
        return LockOutcome(busy=f"repository busy ({describe(first)}{more})")
    if not stale or live:
        return LockOutcome()  # nothing to clear, or only shared locks that do not block
    try:
        unlocked = restic(["unlock"], env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return LockOutcome(busy=f"repository locks could not be cleared "
                                f"({type(exc).__name__}: {exc})")
    if unlocked.returncode != 0:
        return LockOutcome(busy=f"repository locks could not be cleared "
                                f"({_short(unlocked, 'restic unlock failed')})")
    where = ", ".join(sorted({describe(lock) for lock in stale}))
    return LockOutcome(
        cleared=tuple(stale),
        note=f"cleared {len(stale)} stale repository lock(s) before the check ({where})")


# A drill reports only pass/fail to its caller; what it did on the way (a stale lock cleared)
# is parked here by repository URL for the scheduled pass to put in the receipt.
_notes: dict[str, list[str]] = {}
_notes_lock = threading.Lock()


def add_note(repo_url: str, note: str) -> None:
    with _notes_lock:
        _notes.setdefault(repo_url, []).append(note)


def pop_notes(repo_url: str) -> list[str]:
    with _notes_lock:
        return _notes.pop(repo_url, [])


def is_lock_failure(text: str) -> bool:
    """Does a failed restic run's output say the repository was locked?"""
    return "already locked" in text or "unable to create lock" in text
