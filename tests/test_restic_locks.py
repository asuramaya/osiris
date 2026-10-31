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

cmd = args[0] if args else ""
if cmd == "list" and args[1] == "locks":
    if state.get("list_fails"):
        save(); print("Fatal: wrong password or no key found", file=sys.stderr); sys.exit(1)
    print("\\n".join(state["locks"]))
elif cmd == "cat" and args[1] == "lock":
    print(json.dumps(state["locks"][args[2]]))
elif cmd == "unlock":
    if state.get("unlock_fails"):
        save(); print("Fatal: unlock refused", file=sys.stderr); sys.exit(1)
    now = datetime.now(timezone.utc)
    keep = {{}}
    for lid, body in state["locks"].items():
        age = now - datetime.fromisoformat(body["time"])
        if age <= timedelta(minutes=30):
            keep[lid] = body
    state["locks"] = keep
elif cmd == "check":
    if state["locks"]:
        save()
        print("Fatal: unable to create lock in backend: repository is already locked exclusively",
              file=sys.stderr)
        sys.exit(1)
elif cmd == "snapshots":
    pass
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

    assert out == restic_locks.LockOutcome() and "unlock" not in fake.commands()


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
