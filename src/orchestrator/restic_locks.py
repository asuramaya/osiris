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
import signal
import socket
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

STALE_AFTER = timedelta(minutes=30)
CALL_TIMEOUT_SECONDS = 30  # the same bound the offload runner gives `restic snapshots`
# How long a restic we are stopping gets to remove its own lock (restic unlocks on SIGINT/SIGTERM)
# before it is killed. A SIGKILL leaves the lock behind, which is how osiris's own runs kept
# locking the repository against the next one.
STOP_GRACE_SECONDS = 15

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
    unreadable: str | None = None  # why the lock listing could not be read, when it could not


def stop_restic(proc: subprocess.Popen[Any], grace: float | None = None) -> None:
    """Ends a restic process the way that leaves no lock behind: SIGINT (it unlocks and exits),
    a short wait, and a SIGKILL only if it ignored that. Safe on one that already exited."""
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=STOP_GRACE_SECONDS if grace is None else grace)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def run_restic(args: list[str], env: dict[str, str], *, timeout: float, text: bool = True,
               grace: float | None = None) -> subprocess.CompletedProcess[Any]:
    """`subprocess.run(["restic", *args], timeout=...)` without its habit of SIGKILLing a restic
    that holds the repository lock: on timeout the process is asked to stop first
    (`stop_restic`), and the same `TimeoutExpired` is raised for the caller."""
    cmd = ["restic", *args]
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=text)
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        stop_restic(proc, grace)
        out, err = proc.communicate()  # unbounded-wait-ok: stop_restic has ended the process
        raise subprocess.TimeoutExpired(cmd, timeout, output=out, stderr=err) from None
    except BaseException:
        stop_restic(proc, grace)
        raise
    return subprocess.CompletedProcess(cmd, proc.returncode, out, err)


def _run(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return run_restic(args, env, timeout=CALL_TIMEOUT_SECONDS)


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
    listed = restic(["list", "locks", "--quiet", "--no-lock"], env)
    if listed.returncode != 0:
        return _short(listed, "could not list the repository's locks")
    locks: list[Lock] = []
    for lock_id in listed.stdout.split():
        # --no-lock matters: `cat` otherwise takes its own lock, which an EXCLUSIVE lock refuses
        # ("repository is already locked exclusively ... the `unlock` command can be used"), so the
        # one case this module exists for was the one case it could not read (the NAS, 2026-11-06)
        got = restic(["cat", "lock", "--no-lock", lock_id], env)
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


# A line that names WHY the connection or login failed, as opposed to restic's own closing line.
_CAUSE_MARKERS = (
    "ssh:", "no route to host", "network is unreachable", "network unreachable",
    "connection refused", "connection timed out", "connection reset", "could not resolve",
    "name or service not known", "host key verification", "permission denied",
    "broken pipe", "operation timed out", "i/o timeout",
)


def failure_summary(text: str) -> str:
    """One line for a failed restic run that keeps the CAUSE. restic ends a failed sftp run with a
    generic line ("Fatal: unable to open repository at ...: unexpected EOF"); the ssh error that
    explains it ("ssh: connect to host nas port 22: No route to host") is the line BEFORE. Keeping
    only the last line hid that cause, so a run that could not reach the NAS was recorded as a
    failure and never read as "away from home". The summary is the last line, followed by the
    latest earlier line that names a cause, when there is one."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return "no output"
    last = lines[-1]
    cause = next((ln for ln in reversed(lines[:-1])
                  if any(m in ln.lower() for m in _CAUSE_MARKERS)), None)
    if cause and cause != last and not any(m in last.lower() for m in _CAUSE_MARKERS):
        return f"{last[:160]} ({cause[:200]})"
    return last[:240]


def _short(proc: subprocess.CompletedProcess[str], what: str) -> str:
    detail = failure_summary(proc.stderr or proc.stdout or "")
    return f"{what}: {detail}" if (proc.stderr or proc.stdout or "").strip() else what


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
    since = (lock.time.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC") if lock.time
             else "an unknown time")
    return f"lock by {lock.hostname}/{lock.pid if lock.pid is not None else '?'} since {since}"


def clear_stale_locks(
    env: dict[str, str], *, restic: Runner = _run, now: datetime | None = None,
    local_host: str | None = None, pid_alive: Callable[[int], bool] = _pid_alive,
    blocking_only_exclusive: bool = False, after_lock_failure: bool = False,
) -> LockOutcome:
    """Removes the repository's stale locks and reports a live one that blocks.
    `blocking_only_exclusive` is for a backup, which coexists with other shared locks and is
    stopped only by an exclusive one. `after_lock_failure` is for a caller whose own restic call
    has ALREADY failed on a lock: if the listing cannot be read it then runs a plain
    `restic unlock` anyway (restic removes only locks it judges stale), because "could not read
    the locks" must never be the end of the road for a repository that is demonstrably locked."""
    now = now or datetime.now(UTC)
    local_host = local_host or socket.gethostname()
    try:
        locks = read_locks(env, restic)
    except subprocess.TimeoutExpired as exc:
        # the repository did not answer a lock listing in time; every call after it would wait
        # just as long (a slow or unreachable link), so say so now instead of probing again
        return LockOutcome(busy=f"repository not reachable: restic {exc.cmd[1]} timed out "
                                f"after {exc.timeout:.0f}s (a slow or unreachable link)")
    except OSError as exc:
        locks = f"{type(exc).__name__}: {exc}"
    if isinstance(locks, str):
        if not after_lock_failure:
            # Not yet a lock problem (a wrong password, an unreachable repository): the next
            # restic call reports its own error. The reason is kept so that call's failure can
            # carry it if it turns out to be a lock after all.
            return LockOutcome(unreadable=locks)
        return _plain_unlock(env, restic, unreadable=locks)
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
    if not stale:
        return LockOutcome()  # only live shared locks, which do not block this caller
    # Stale locks go even with live shared locks around: `restic unlock` removes only what it
    # judges stale, and a stale EXCLUSIVE lock beside a live shared one still blocks every caller.
    try:
        unlocked = restic(["unlock"], env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return LockOutcome(busy=f"repository locks could not be cleared "
                                f"({type(exc).__name__}: {exc})")
    if unlocked.returncode != 0:
        return LockOutcome(busy=f"repository locks could not be cleared "
                                f"({_short(unlocked, 'restic unlock failed')})")
    # Trust the repository, not the exit code: if restic judged them differently from us the
    # locks are still there, and saying "cleared" would be the lie that hid this bug for days.
    try:
        after = read_locks(env, restic)
    except (OSError, subprocess.TimeoutExpired):
        after = None
    if isinstance(after, list):
        left = [lock for lock in stale if any(lock.id == other.id for other in after)]
        if left:
            first = min(left, key=lambda lock: lock.time or now)
            return LockOutcome(busy=(
                f"restic unlock ran but {len(left)} lock(s) we judge stale remain "
                f"({describe(first)}); restic does not treat them as stale"))
    where = ", ".join(sorted({describe(lock) for lock in stale}))
    return LockOutcome(
        cleared=tuple(stale),
        note=f"cleared {len(stale)} stale repository lock(s) before the check ({where})")


def _plain_unlock(env: dict[str, str], restic: Runner, *, unreadable: str) -> LockOutcome:
    try:
        unlocked = restic(["unlock"], env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return LockOutcome(unreadable=unreadable, busy=(
            f"could not read the repository's locks ({unreadable}) and `restic unlock` did not "
            f"run ({type(exc).__name__}: {exc})"))
    if unlocked.returncode != 0:
        return LockOutcome(unreadable=unreadable, busy=(
            f"could not read the repository's locks ({unreadable}) and `restic unlock` failed "
            f"({_short(unlocked, 'restic unlock failed')})"))
    return LockOutcome(unreadable=unreadable, note=(
        f"could not read the repository's locks ({unreadable}); ran `restic unlock`, which "
        "removes only the locks restic itself judges stale"))


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


# Every message that means "somebody else holds this repository right now" starts with this, so the
# callers can tell a busy repository (a quiet SKIP: try again later, nothing failed) from a real
# failure. The tick and the drill used to collide on the repository (`restic check` is exclusive),
# and the loser was recorded as a failure: a drill held off for six hours, a backup that never ran
# reading `ok: False`.
BUSY_PREFIX = "repository busy"


def is_busy(message: str | None) -> bool:
    """Is this failure string a busy-repository answer (see BUSY_PREFIX)?"""
    return bool(message) and str(message).startswith(BUSY_PREFIX)


def holder_line(text: str) -> str | None:
    """restic's own account of who holds the repository ("repository is already locked
    exclusively by PID 12 on host by user ... lock was created at ..."), from a failed run's
    output, or None. The LAST line of that output ("the `unlock` command can be used ...") is
    advice, not a fact, and is all a one-line error used to keep."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        if "already locked" in line:
            created = next((x for x in lines[i + 1:i + 3] if x.startswith("lock was created")), "")
            return f"{line}; {created}" if created else line
    return None


def is_lock_failure(text: str) -> bool:
    """Does a failed restic run's output say the repository was locked?"""
    return "already locked" in text or "unable to create lock" in text
