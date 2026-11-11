"""Stale restic repository locks are cleared by the tooling, and a live one is named plainly.
Driven by a fake `restic` on PATH (never a real repository): it keeps its locks in a state file,
refuses `check` while any lock exists, and its `unlock` removes only locks older than thirty
minutes, like the real one."""
from __future__ import annotations

import json
import os
import stat
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from scripts import osiris_offbox_restore_drill as drill
from src.orchestrator import offload_runner, restic_locks, scheduled_drill, soul_key

_FAKE = '''#!{python}
import json, os, sys
from datetime import datetime, timedelta, timezone

state_path = os.environ["FAKE_RESTIC_STATE"]
state = json.load(open(state_path))
args = sys.argv[1:]
state["calls"].append(args)

def save():
    json.dump(state, open(state_path, "w"))

no_lock = "--no-lock" in args
args = [a for a in args if a != "--no-lock"]
cmd = args[0] if args else ""

LOCKED = chr(10).join([
    "unable to create lock in backend: repository is already locked exclusively by PID 4242 "
    "on nas-box by u (UID 1, GID 1)",
    "lock was created at 2026-11-06 00:00:00 (2h ago)",
    "storage ID aaaaaaaa",
    "the `unlock` command can be used to remove stale locks"])

def exclusive_present():
    return any(b.get("exclusive") for b in state["locks"].values())

def refuse_if_locked():
    # the real restic: every command that takes its own lock is refused by an EXCLUSIVE lock
    # (`list locks` is exempt; `cat lock` is not, unless --no-lock)
    if not no_lock and exclusive_present():
        save(); print(LOCKED, file=sys.stderr); sys.exit(1)

if cmd == "list" and args[1] == "locks":
    if state.get("list_sleep"):
        save(); import time; time.sleep(state["list_sleep"])
    if state.get("list_fails") or state.pop("list_fails_once", False):
        save(); print("Fatal: wrong password or no key found", file=sys.stderr); sys.exit(1)
    print("\\n".join(state["locks"]))
elif cmd == "cat" and args[1] == "lock":
    refuse_if_locked()
    print(json.dumps(state["locks"][args[2]]))
elif cmd == "unlock":
    if state.get("unlock_fails"):
        save(); print("Fatal: unlock refused", file=sys.stderr); sys.exit(1)
    if state.get("unlock_noop"):
        save(); sys.exit(0)
    now = datetime.now(timezone.utc)
    keep = {{}}
    for lid, body in state["locks"].items():
        age = now - datetime.fromisoformat(body["time"])
        # restic's rule: older than thirty minutes, or a process that is gone on THIS machine
        if age <= timedelta(minutes=30) and lid not in state.get("dead_local", []):
            keep[lid] = body
    state["locks"] = keep
elif cmd == "check":
    if state.get("check_always_locked"):
        save(); print(LOCKED, file=sys.stderr); sys.exit(1)
    if state.get("check_locks_once"):
        lid = state.pop("check_locks_once")
        old = (datetime.now(timezone.utc) - timedelta(minutes=400)).isoformat()
        state["locks"][lid] = {{"time": old, "exclusive": True, "hostname": "nas-box",
                               "username": "u", "pid": 4242}}
        save(); print(LOCKED, file=sys.stderr); sys.exit(1)
    if state["locks"]:
        save()
        print("Fatal: unable to create lock in backend: repository is already locked exclusively",
              file=sys.stderr)
        sys.exit(1)
elif cmd == "snapshots":
    if state.get("snapshots_locked"):
        save(); print(LOCKED, file=sys.stderr); sys.exit(1)
    refuse_if_locked()
elif cmd == "ls":
    pass
elif cmd == "backup":
    state["backups"] = state.get("backups", 0) + 1
save()
'''


class Fake:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.state_path = tmp_path / "state.json"
        self.state: dict[str, Any] = {"locks": {}, "calls": []}
        self._write()
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        exe = bin_dir / "restic"
        exe.write_text(_FAKE.format(python=sys.executable))
        exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
        monkeypatch.setenv("FAKE_RESTIC_STATE", str(self.state_path))
        monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "a-test-password")
        self.env = {**os.environ, "RESTIC_REPOSITORY": "fake", "RESTIC_PASSWORD": "x"}

    def _write(self) -> None:
        self.state_path.write_text(json.dumps(self.state))

    def lock(self, lock_id: str, *, age_min: float, host: str = "nas-box", pid: int = 4242,
             exclusive: bool = True) -> None:
        when = datetime.now(UTC) - timedelta(minutes=age_min)
        self.state["locks"][lock_id] = {"time": when.isoformat(), "exclusive": exclusive,
                                        "hostname": host, "username": "u", "pid": pid}
        self._write()

    def read(self) -> dict[str, Any]:
        return dict(json.loads(self.state_path.read_text()))

    def commands(self) -> list[str]:
        return [c[0] for c in self.read()["calls"]]


@pytest.fixture
def fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Fake:
    return Fake(tmp_path, monkeypatch)


def test_a_stale_lock_by_age_is_cleared_with_a_plain_unlock(fake: Fake) -> None:
    fake.lock("a" * 64, age_min=300)

    out = restic_locks.clear_stale_locks(fake.env, local_host="this-box")

    assert out.busy is None and len(out.cleared) == 1
    assert out.note and "cleared 1 stale repository lock" in out.note and "nas-box/4242" in out.note
    assert fake.read()["locks"] == {}
    assert ["unlock"] in fake.read()["calls"]  # plain unlock, never --remove-all


def test_a_fresh_lock_by_a_dead_process_on_this_machine_is_stale(fake: Fake) -> None:
    fake.lock("b" * 64, age_min=2, host="this-box", pid=999_999)
    fake.state["dead_local"] = ["b" * 64]
    fake._write()
    out = restic_locks.clear_stale_locks(
        fake.env, local_host="this-box", pid_alive=lambda pid: False)
    assert out.busy is None and len(out.cleared) == 1


def test_a_fresh_lock_by_a_live_process_is_named_not_cleared(fake: Fake) -> None:
    fake.lock("c" * 64, age_min=3, host="other-box", pid=77)

    out = restic_locks.clear_stale_locks(fake.env, local_host="this-box")

    assert out.busy is not None
    assert out.busy.startswith("repository busy (lock by other-box/77 since ")
    assert out.cleared == () and "unlock" not in fake.commands()
    assert "Fatal" not in out.busy  # a sentence, never a raw stderr dump


def test_one_live_lock_among_stale_ones_clears_nothing(fake: Fake) -> None:
    fake.lock("d" * 64, age_min=500)
    fake.lock("e" * 64, age_min=1, host="other-box", pid=5)

    out = restic_locks.clear_stale_locks(fake.env, local_host="this-box")

    assert out.busy is not None and "other-box/5" in out.busy
    assert "unlock" not in fake.commands() and len(fake.read()["locks"]) == 2


def test_an_unreadable_lock_listing_is_left_to_the_next_call_to_report(fake: Fake) -> None:
    fake.state["list_fails"] = True
    fake._write()

    out = restic_locks.clear_stale_locks(fake.env)

    # nothing is cleared or claimed busy, but the reason is KEPT so a later lock failure can
    # carry it (it used to be dropped, which is how the NAS loop stayed unexplained)
    assert out.busy is None and out.cleared == () and out.note is None
    assert out.unreadable and "wrong password" in out.unreadable
    assert "unlock" not in fake.commands()


def test_a_repository_with_no_locks_is_left_alone(fake: Fake) -> None:
    out = restic_locks.clear_stale_locks(fake.env)
    assert out == restic_locks.LockOutcome() and "unlock" not in fake.commands()


def test_a_backup_is_stopped_only_by_an_exclusive_live_lock(fake: Fake) -> None:
    fake.lock("f" * 64, age_min=1, host="other-box", exclusive=False)
    shared = restic_locks.clear_stale_locks(
        fake.env, local_host="this-box", blocking_only_exclusive=True)
    assert shared.busy is None

    fake.lock("0" * 64, age_min=1, host="other-box", exclusive=True)
    exclusive = restic_locks.clear_stale_locks(
        fake.env, local_host="this-box", blocking_only_exclusive=True)
    assert exclusive.busy is not None


def test_a_failed_unlock_is_reported_not_swallowed(fake: Fake) -> None:
    fake.lock("1" * 64, age_min=400)
    fake.state["unlock_fails"] = True
    fake._write()

    out = restic_locks.clear_stale_locks(fake.env, local_host="this-box")

    assert out.busy is not None and "could not be cleared" in out.busy


def test_the_bounded_drill_clears_a_stale_lock_and_notes_it(fake: Fake) -> None:
    fake.lock("2" * 64, age_min=900)

    failure = drill.run_bounded_drill("fake://repo")

    assert failure is not None and "locked" not in failure  # it got past the check step
    assert "check" in fake.commands()
    assert any("cleared 1 stale repository lock" in n for n in restic_locks.pop_notes("fake://repo"))


def test_the_bounded_drill_names_a_live_holder_instead_of_dumping_stderr(fake: Fake) -> None:
    fake.lock("3" * 64, age_min=2, host="other-box", pid=31337)

    failure = drill.run_bounded_drill("fake://repo")

    assert failure is not None and failure.startswith("repository busy (lock by other-box/31337")
    assert "Fatal" not in failure and "check" not in fake.commands()


def test_the_full_drill_does_the_same(fake: Fake) -> None:
    fake.lock("4" * 64, age_min=2, host="other-box", pid=9)
    failure = drill.run_drill("fake://repo")
    assert failure is not None and failure.startswith("repository busy (lock by other-box/9")


def test_a_lock_that_appears_after_the_pre_check_is_still_named(
    fake: Fake, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pre-check saw an empty repository; the check then hit a live lock."""
    real = restic_locks.clear_stale_locks
    calls = {"n": 0}

    def _late(env: dict[str, str], **kw: Any) -> restic_locks.LockOutcome:
        calls["n"] += 1
        if calls["n"] == 1:
            return restic_locks.LockOutcome()  # nothing there at the pre-check
        return real(env, **kw)

    monkeypatch.setattr(restic_locks, "clear_stale_locks", _late)
    fake.lock("5" * 64, age_min=1, host="other-box", pid=11)

    failure = drill.run_bounded_drill("fake://repo")

    assert failure is not None and failure.startswith("repository busy (lock by other-box/11")


@pytest.fixture
def receipts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "drill_receipts.json"
    monkeypatch.setenv("OSIRIS_RESTORE_DRILL_RECEIPTS_FILE", str(path))
    return path


async def test_the_scheduled_pass_records_the_cleared_lock_in_the_receipt(
    receipts: Path,
) -> None:
    def _drill(url: str) -> str | None:
        restic_locks.add_note(url, "cleared 2 stale repository lock(s) before the check (x)")
        return None

    out = await scheduled_drill.run_due_drills(
        [{"name": "nas", "path_or_url": "sftp:nas:/r"}],
        {"nas": {"last_successful_offload": datetime.now(UTC).isoformat()}},
        run_drill=_drill)

    assert out[0]["ok"] is True and "cleared 2 stale" in out[0]["note"]
    assert "cleared 2 stale" in soul_key.restore_drill_receipts()["sftp:nas:/r"]["last_note"]


def test_a_backup_clears_a_stale_lock_and_runs(fake: Fake, tmp_path: Path) -> None:
    fake.lock("6" * 64, age_min=700)
    source = tmp_path / "vault"
    source.mkdir()

    failure = offload_runner._run_restic_backup(
        repository="fake", password=b"x", source=source)

    assert failure is None and fake.read()["backups"] == 1


def test_a_backup_names_a_live_exclusive_holder(fake: Fake, tmp_path: Path) -> None:
    fake.lock("7" * 64, age_min=1, host="other-box", pid=8)
    source = tmp_path / "vault"
    source.mkdir()

    failure = offload_runner._run_restic_backup(
        repository="fake", password=b"x", source=source)

    assert failure is not None and failure.startswith("repository busy (lock by other-box/8")
    assert fake.read().get("backups", 0) == 0


def test_a_lock_listing_that_times_out_ends_the_call_without_a_second_wait(
    fake: Fake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreachable repository answers nothing: the lock listing's bound fires, and the backup
    says so at once instead of waiting the same time again on the probe (and never initialises)."""
    monkeypatch.setattr(restic_locks, "CALL_TIMEOUT_SECONDS", 1)
    fake.state["list_sleep"] = 4
    fake._write()
    source = tmp_path / "vault"
    source.mkdir()

    failure = offload_runner._run_restic_backup(
        repository="fake", password=b"x", source=source)

    assert failure is not None and failure.startswith("repository not reachable: restic list")
    assert "timed out after 1s" in failure
    assert fake.commands() == ["list"]  # no snapshots, no init, no backup


# ---- the NAS loop (2026-11-06 to 11-10): 111 offload ticks failed on a lock that was never cleared
# or named, and 5 drills said "the stale lock was cleared" while the next tick was locked again ----


def test_an_exclusive_lock_can_be_read_because_listing_never_takes_a_lock(fake: Fake) -> None:
    """ROOT CAUSE. Real restic's `cat lock` takes its own lock unless given --no-lock, and an
    EXCLUSIVE lock refuses it ("already locked exclusively ... the `unlock` command can be used
    to remove stale locks"), so the listing failed exactly when an exclusive lock existed and the
    tooling stayed out of the way. Verified against restic 0.18.1; the fake now behaves alike."""
    fake.lock("a" * 64, age_min=300, exclusive=True)

    locks = restic_locks.read_locks(fake.env)

    assert isinstance(locks, list) and len(locks) == 1 and locks[0].exclusive
    cats = [c for c in fake.read()["calls"] if c[:2] == ["cat", "lock"]]
    assert cats and all("--no-lock" in c for c in cats)


def test_a_stale_exclusive_lock_beside_a_live_shared_one_is_still_cleared(fake: Fake) -> None:
    """The early return `if not stale or live` left a stale exclusive lock in place whenever any
    live shared lock existed; restic unlock removes only stale ones, so it is always safe."""
    fake.lock("b" * 64, age_min=400, exclusive=True)
    fake.lock("c" * 64, age_min=1, host="other-box", pid=9, exclusive=False)

    out = restic_locks.clear_stale_locks(
        fake.env, local_host="this-box", blocking_only_exclusive=True)

    assert out.busy is None and [lock.id for lock in out.cleared] == ["b" * 64]
    assert list(fake.read()["locks"]) == ["c" * 64]  # the live shared lock stays


def test_a_clear_restic_did_not_perform_is_not_reported_as_cleared(fake: Fake) -> None:
    """The result is checked against the repository: when restic's own staleness rule disagrees
    (here: the lock is old by our clock only) the answer is a plain statement, never "cleared"."""
    fake.lock("d" * 64, age_min=400, exclusive=True)
    fake.state["unlock_noop"] = True
    fake._write()

    out = restic_locks.clear_stale_locks(fake.env, local_host="this-box")

    assert out.cleared == () and out.note is None
    assert out.busy and "restic unlock ran but 1 lock(s)" in out.busy


def test_an_unreadable_listing_after_a_lock_failure_still_runs_a_plain_unlock(
    fake: Fake,
) -> None:
    fake.lock("e" * 64, age_min=400, exclusive=True)
    fake.state["list_fails"] = True
    fake._write()

    out = restic_locks.clear_stale_locks(fake.env, after_lock_failure=True)

    assert out.busy is None and out.note and "ran `restic unlock`" in out.note
    assert fake.read()["locks"] == {}


def test_the_backup_recovers_when_the_pre_check_could_not_read_the_locks(
    fake: Fake, tmp_path: Path,
) -> None:
    """The loop itself: listing fails, `snapshots` then fails on the stale exclusive lock; the
    runner now clears it and goes on to back up instead of recording the raw restic text."""
    fake.lock("f" * 64, age_min=400, exclusive=True)
    fake.state["list_fails_once"] = True
    fake._write()
    source = tmp_path / "vault"
    source.mkdir()

    failure = offload_runner._run_restic_backup(
        repository="fake", password=b"x", source=source)

    assert failure is None and fake.read()["backups"] == 1 and fake.read()["locks"] == {}


def test_a_lock_failure_names_restics_own_holder_line_not_its_advice(
    fake: Fake, tmp_path: Path,
) -> None:
    """The recorded error used to be restic's LAST line ("the `unlock` command can be used ..."):
    advice, with the holder nowhere in it. It names who holds the repository now."""
    fake.lock("1" * 64, age_min=1, host="nas-box", pid=4242, exclusive=False)
    fake.state["snapshots_locked"] = True  # a lock that appears after the pre-check
    fake._write()
    source = tmp_path / "vault"
    source.mkdir()

    failure = offload_runner._run_restic_backup(
        repository="fake", password=b"x", source=source)

    assert failure is not None and restic_locks.is_busy(failure)  # a busy repository, not a failure
    assert "already locked exclusively by PID 4242 on nas-box" in failure
    assert "the `unlock` command" not in failure
    assert "init" not in fake.commands()


def test_the_drill_clears_and_retries_once_instead_of_claiming_a_clear(
    fake: Fake, monkeypatch: pytest.MonkeyPatch,
) -> None:
    restic_locks.pop_notes("fake")
    fake.state["check_locks_once"] = "f" * 64  # a stale exclusive lock appears after the pre-check
    fake._write()

    failure = drill.run_bounded_drill("fake")

    # the check passed on its retry; the drill then stops for an unrelated reason (the fake
    # repository has no snapshots), and the clear is recorded for the receipt
    assert failure is not None and failure.startswith("the latest snapshot has no files")
    assert fake.commands().count("check") == 2 and fake.commands().count("unlock") >= 1
    assert any("cleared 1 stale" in n for n in restic_locks.pop_notes("fake"))


def test_the_drill_says_so_when_the_repository_is_locked_and_nothing_is_stale(
    fake: Fake,
) -> None:
    fake.lock("2" * 64, age_min=1, host="other-box", pid=11, exclusive=False)
    fake.state["check_always_locked"] = True
    fake._write()

    failure = drill.run_bounded_drill("fake")

    assert failure is not None and "cleared" not in failure
    assert failure.startswith("repository busy (lock by other-box/11")  # the pre-check names it


def test_describe_prints_utc_whatever_zone_the_lock_was_written_in() -> None:
    from datetime import timezone

    when = datetime(2026, 11, 11, 11, 55, tzinfo=timezone(timedelta(hours=-6)))
    lock = restic_locks.Lock(id="x", time=when, hostname="h", pid=1, exclusive=False)
    assert restic_locks.describe(lock) == "lock by h/1 since 2026-11-11 17:55 UTC"


def test_holder_line_picks_restics_account_of_the_holder() -> None:
    text = ("unable to create lock in backend: repository is already locked exclusively by PID 9 "
            "on box by u (UID 1, GID 1)\nlock was created at 2026-11-06 00:00:00 (2h ago)\n"
            "storage ID aaaa\nthe `unlock` command can be used to remove stale locks")
    held = restic_locks.holder_line(text)
    assert held and "PID 9 on box" in held and "lock was created at 2026-11-06" in held
    assert restic_locks.holder_line("Fatal: wrong password") is None


# ---- no osiris run leaves a lock behind: a stopped restic is asked to unlock itself first ----

_SLEEPER = '''#!{python}
import os, signal, sys, time
marker = os.environ["FAKE_RESTIC_MARKER"]
open(marker, "w").write("holding")
if os.environ.get("FAKE_RESTIC_CHILD_PID_FILE"):
    # like the ssh behind an sftp repository: a child that inherits our pipes and outlives us
    import subprocess
    child = subprocess.Popen(["sleep", "60"])
    open(os.environ["FAKE_RESTIC_CHILD_PID_FILE"], "w").write(str(child.pid))
if os.environ.get("FAKE_RESTIC_IGNORE_SIGINT"):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
else:
    def release(signum, frame):
        open(marker, "w").write("released")  # restic removes its lock on SIGINT/SIGTERM
        sys.exit(130)
    signal.signal(signal.SIGINT, release)
time.sleep(60)
'''


@pytest.fixture
def sleeper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    bin_dir = tmp_path / "sleepbin"
    bin_dir.mkdir()
    exe = bin_dir / "restic"
    exe.write_text(_SLEEPER.format(python=sys.executable))
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    marker = tmp_path / "marker"
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_RESTIC_MARKER", str(marker))
    return marker


def test_a_timed_out_restic_is_asked_to_release_its_lock_not_killed(sleeper: Path) -> None:
    import subprocess

    with pytest.raises(subprocess.TimeoutExpired) as info:
        restic_locks.run_restic(["backup"], dict(os.environ), timeout=1.0)

    assert info.value.cmd == ["restic", "backup"] and info.value.timeout == 1.0
    # it ran its own unlock; a SIGKILL would have left "holding"
    assert sleeper.read_text() == "released"


def test_a_restic_that_ignores_the_request_is_killed_after_the_grace(
    sleeper: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import subprocess
    import time

    monkeypatch.setenv("FAKE_RESTIC_IGNORE_SIGINT", "1")
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        restic_locks.run_restic(["backup"], dict(os.environ), timeout=1.0, grace=0.5)

    assert time.monotonic() - started < 6.0 and sleeper.read_text() == "holding"


def test_the_drill_readers_stop_restic_without_a_kill(sleeper: Path) -> None:
    """`restic ls` / `restic dump` are stopped early on purpose (only the first bytes are wanted);
    a SIGKILL there left a lock on every drill."""
    import subprocess
    import time

    proc = subprocess.Popen(["restic", "dump"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for _ in range(100):
        if sleeper.exists():
            break
        time.sleep(0.05)
    drill._stop(proc)

    assert proc.poll() is not None and sleeper.read_text() == "released"



# ---- a busy repository is a quiet skip, and the tick and the restore test never meet ----


async def _nas_target(actions):  # type: ignore[no-untyped-def]
    from src.orchestrator.backup_settings import write_backup_settings

    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic", "path_or_url": "sftp://nas/repo",
                          "schedule": "*-*-* 03:00:00", "enabled": True}])


async def test_a_tick_that_finds_the_repository_busy_skips_and_records_no_failure(
    actions, fake: Fake, monkeypatch: pytest.MonkeyPatch,  # type: ignore[no-untyped-def]
) -> None:
    """The restore test holds the repository (restic check is exclusive); the backup tick that
    lands beside it used to record `ok: False` and a last_error for a backup that never ran."""
    from src.orchestrator import network_presence

    monkeypatch.setattr(network_presence, "lan_presence", lambda target: {"present": True})
    monkeypatch.setattr(offload_runner, "get_restic_password", lambda: b"x", raising=False)
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "x")
    await _nas_target(actions)
    offload_runner._write_receipt("nas", {"last_successful_offload": "T0", "last_attempt_at": "T0",
                                          "last_error": None})
    fake.lock("a" * 64, age_min=1, host="nas-box", pid=77, exclusive=True)  # live, someone else's

    out = await offload_runner.run_offload_tick(actions.pool)

    [row] = out["targets"]
    assert "skipped" in row and restic_locks.is_busy(row["skipped"])
    assert "nas-box/77" in row["skipped"]
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["last_successful_offload"] == "T0"  # never clobbered
    assert not receipt.get("last_error")                # and no failure recorded
    assert "backup" not in fake.commands()


async def test_a_drill_that_finds_the_repository_busy_records_nothing_and_stays_due(
    actions, fake: Fake,  # type: ignore[no-untyped-def]
) -> None:
    from src.orchestrator import scheduled_drill, soul_key

    fake.lock("b" * 64, age_min=1, host="nas-box", pid=78, exclusive=True)
    present = [{"name": "nas", "path_or_url": "fake"}]
    offload = {"nas": {"last_successful_offload": datetime.now(UTC).isoformat()}}

    first = await scheduled_drill.run_due_drills(
        present, offload, run_drill=drill.run_bounded_drill)

    [row] = first
    assert "skipped" in row and restic_locks.is_busy(row["skipped"])
    assert soul_key.restore_drill_receipts() == {}  # no receipt: no failure, no six-hour hold-off
    fake.state["locks"] = {}
    fake._write()
    again = await scheduled_drill.run_due_drills(
        present, offload, run_drill=drill.run_bounded_drill)  # due at once, not held off
    assert again and "skipped" not in again[0]


async def test_the_drill_pass_does_nothing_while_the_tick_holds_the_lock(
    actions, fake: Fake, monkeypatch: pytest.MonkeyPatch,  # type: ignore[no-untyped-def]
) -> None:
    from src.orchestrator import recovery_copies, scheduled_drill

    async def present(pool):  # type: ignore[no-untyped-def]
        return [{"name": "nas", "path_or_url": "fake"}]

    monkeypatch.setattr(recovery_copies, "present_targets", present)
    with offload_runner.hold_tick_lock() as ours:
        assert ours
        out = await scheduled_drill.run_drill_pass(actions.pool)
    assert out == {"drills": [], "skipped": "the offload tick is running"}
    assert fake.commands() == []  # restic never ran
    # and once the tick is done the drill runs
    again = await scheduled_drill.run_drill_pass(actions.pool)
    assert "skipped" not in again


async def test_a_tick_started_while_the_drill_holds_the_lock_skips_as_already_running(
    actions, fake: Fake,  # type: ignore[no-untyped-def]
) -> None:
    with offload_runner.hold_tick_lock() as ours:
        assert ours
        out = await offload_runner.run_offload_tick(actions.pool)
    assert out == {"skipped": "already running", "targets": []}
    assert fake.commands() == []


def test_a_child_that_keeps_the_pipes_open_cannot_hang_the_run_after_restic_is_stopped(
    sleeper: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a timeout restic is stopped, but the ssh process behind an sftp repository can
    still hold the output pipes, and reading them to the end waited on a dead link forever (the
    unit has no start timeout either). The read after the stop is bounded, the pipes are closed,
    and the leftover group is ended."""
    import subprocess
    import time

    pid_file = tmp_path / "child.pid"
    monkeypatch.setenv("FAKE_RESTIC_CHILD_PID_FILE", str(pid_file))
    monkeypatch.setattr(restic_locks, "POST_STOP_DRAIN_SECONDS", 1)
    started = time.monotonic()

    with pytest.raises(subprocess.TimeoutExpired):
        restic_locks.run_restic(["backup"], dict(os.environ), timeout=1.0, grace=3.0)

    assert time.monotonic() - started < 15.0  # not the child's 60 s
    child = int(pid_file.read_text())
    for _ in range(50):  # the leftover child is ended with the group
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        os.kill(child, 9)
        raise AssertionError("the child that held the pipes was left running")
