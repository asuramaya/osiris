"""`osiris nas-first-offload`: six steps, one plain line each, stopping at the first that fails.
Run against fakes: no restic, no network, no NAS."""
from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import nas_first_offload as nas

NOW = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)
NAS = {"name": "nas", "kind": "restic", "enabled": True,
       "path_or_url": "sftp:truenas:/mnt/Bunker/Vault/osiris-restic/repo"}


class World:
    """A fake of everything the command touches, recording what was done."""

    def __init__(self, **over: Any) -> None:
        self.calls: list[str] = []
        self.home: dict[str, Any] = {"present": True, "reason": None, "dev": "enp0s31f6"}
        self.state: tuple[str, Any] = ("ok", 3)
        self.init_fail: str | None = None
        self.offload_out: tuple[str | None, str | None] = (None, None)
        self.snapshot: dict[str, Any] | None = {
            "id": "ab12cd34", "time": (NOW - timedelta(minutes=2)).isoformat(),
            "paths": ["/home/u/osiris-vault"]}
        self.drill_fail: str | None = None
        self.copy: dict[str, Any] | None = {"dest": "nas", "ok": True}
        self.offloaded_before = True
        self.locked = True
        self.recorded: list[tuple[str, Any]] = []
        for k, v in over.items():
            setattr(self, k, v)

    def deps(self) -> nas.Deps:
        def mark(name: str, value: Any) -> Any:
            self.calls.append(name)
            return value

        @contextlib.contextmanager
        def lock() -> Any:
            yield self.locked

        return nas.Deps(
            presence=lambda t: mark("presence", self.home),
            repo_state=lambda r, p: mark("repo_state", self.state),
            init_repo=lambda r, p: mark("init", self.init_fail),
            offload=lambda r, p, s: mark("offload", self.offload_out),
            latest_snapshot=lambda r, p: mark("snapshot", self.snapshot),
            drill=lambda r: mark("drill", self.drill_fail),
            copy_recovery=lambda t: mark("copy", self.copy),
            ever_offloaded=lambda n: self.offloaded_before,
            record_offload=lambda n, now, note: self.recorded.append(("offload", note)),
            record_drill=lambda u, fail, note: self.recorded.append(("drill", fail)),
            hold_lock=lock, now=lambda: NOW)


async def _run(world: World, **kw: Any) -> tuple[int, list[str]]:
    lines: list[str] = []
    rc = await nas.run(NAS, b"pw", Path("/home/u/osiris-vault"), world.deps(),
                       say=lines.append, **kw)
    return rc, lines


async def test_at_home_every_step_runs_in_order_with_one_line_each() -> None:
    world = World()

    rc, lines = await _run(world)

    assert rc == 0
    assert world.calls == ["presence", "repo_state", "offload", "snapshot", "drill", "copy"]
    steps = [ln for ln in lines if ln.startswith("[")]
    assert [s[:5] for s in steps][:2] == ["[1/6]", "[2/6]"]
    assert steps[-1][:5] == "[6/6]"
    assert any("enp0s31f6" in ln for ln in lines)  # the interface is named
    assert lines[-1].startswith("done:")
    assert ("offload", None) in world.recorded and ("drill", None) in world.recorded


async def test_away_from_home_it_stops_before_touching_anything() -> None:
    world = World(home={"present": False, "reason": "reachable only over tailscale0"})

    rc, lines = await _run(world)

    assert rc == 1 and world.calls == ["presence"]  # no restic, no upload
    assert "not on the home network (reachable only over tailscale0)" in lines[0]
    assert "when you are home on the LAN, not over Tailscale" in lines[1]


async def test_a_route_it_cannot_confirm_is_refused_not_assumed() -> None:
    # lan_presence says "present" when it cannot find out; this command wants the proof
    world = World(home={"present": True, "reason": None, "dev": None})

    rc, lines = await _run(world)

    assert rc == 1 and world.calls == ["presence"]
    assert "not going without that proof" in lines[0]


async def test_a_running_offload_or_drill_is_not_collided_with() -> None:
    world = World(locked=False)

    rc, lines = await _run(world)

    assert rc == 1 and world.calls == ["presence"]
    assert "is running right now" in lines[-2]


@pytest.mark.parametrize("state,expect", [
    (("busy", "locked by another run"), "in use (locked by another run)"),
    (("unreachable", "No route to host"), "NAS did not answer (No route to host)"),
    (("error", "wrong password"), "could not read the repository (wrong password)"),
])
async def test_a_repository_that_cannot_be_read_stops_at_step_two(
    state: tuple[str, str], expect: str,
) -> None:
    world = World(state=state)

    rc, lines = await _run(world)

    assert rc == 1 and "offload" not in world.calls and expect in lines[-2]


async def test_a_missing_repository_is_created_only_for_a_target_that_never_offloaded() -> None:
    fresh = World(state=("missing", ""), offloaded_before=False)
    rc, lines = await _run(fresh)
    assert rc == 0 and "init" in fresh.calls and any("was created" in ln for ln in lines)

    old = World(state=("missing", ""), offloaded_before=True)
    rc, lines = await _run(old)
    assert rc == 1 and "init" not in old.calls and "offload" not in old.calls
    assert "has offloaded before; not creating a new one over it" in lines[-2]


async def test_a_repository_that_cannot_be_created_stops() -> None:
    world = World(state=("missing", ""), offloaded_before=False, init_fail="permission denied")

    rc, lines = await _run(world)

    assert rc == 1 and "offload" not in world.calls
    assert "could not create the repository (permission denied)" in lines[-2]


async def test_a_failed_offload_stops_names_the_resume_and_records_no_success() -> None:
    world = World(offload_out=("restic backup failed: timed out", None))

    rc, lines = await _run(world)

    assert rc == 1 and world.calls[-1] == "offload"  # no snapshot check, no drill
    assert "restic resumes from what it already copied" in lines[-1]
    assert world.recorded == []  # nothing is written as a success


async def test_the_offload_note_for_unread_files_reaches_the_record_and_the_line() -> None:
    world = World(offload_out=(None, "snapshot saved, but restic could not read some files"))

    rc, lines = await _run(world)

    assert rc == 0 and ("offload", "snapshot saved, but restic could not read some files") in (
        world.recorded)
    assert any("could not read some files" in ln for ln in lines)


@pytest.mark.parametrize("snap,expect", [
    (None, "shows no snapshot after the offload"),
    ({"id": "old00001", "time": (NOW - timedelta(days=3)).isoformat(), "paths": []},
     "not from this run"),
])
async def test_without_a_fresh_snapshot_it_stops_before_the_drill(
    snap: dict[str, Any] | None, expect: str,
) -> None:
    world = World(snapshot=snap)

    rc, lines = await _run(world)

    assert rc == 1 and "drill" not in world.calls and expect in lines[-2]


async def test_a_failed_restore_test_says_the_backup_is_not_proved_and_skips_the_copy() -> None:
    world = World(drill_fail="restic check failed")

    rc, lines = await _run(world)

    assert rc == 1 and "copy" not in world.calls
    assert "the restore test failed (restic check failed)" in lines[-2]
    assert "NOT proved restorable" in lines[-1]
    assert ("drill", "restic check failed") in world.recorded  # the failure is recorded too


@pytest.mark.parametrize("copy,expect_rc,line", [
    (None, 0, "already current"),
    ({"dest": "nas", "ok": True}, 0, "was copied to the NAS"),
    ({"dest": "nas", "ok": False, "error": "sftp refused"}, 1, "not copied (sftp refused)"),
])
async def test_the_recovery_file_step(
    copy: dict[str, Any] | None, expect_rc: int, line: str,
) -> None:
    world = World(copy=copy)

    rc, lines = await _run(world)

    assert rc == expect_rc and any(line in ln for ln in lines)


async def test_check_only_runs_the_two_read_only_steps_and_uploads_nothing() -> None:
    world = World()

    rc, lines = await _run(world, check_only=True)

    assert rc == 0 and world.calls == ["presence", "repo_state"]
    assert lines[-2].startswith("check only") and "nas-first-offload" in lines[-1]


async def test_check_only_on_a_missing_repository_does_not_create_it() -> None:
    world = World(state=("missing", ""), offloaded_before=False)

    rc, lines = await _run(world, check_only=True)

    assert rc == 0 and "init" not in world.calls
    assert any("would create it" in ln for ln in lines)


def test_target_selection() -> None:
    other = {**NAS, "name": "usb", "kind": "local", "path_or_url": "/mnt/usb"}
    second = {**NAS, "name": "nas2", "path_or_url": "sftp:other:/r"}
    off = {**NAS, "name": "nas3", "enabled": False}

    assert nas.pick_target([other, NAS, off], None) == (NAS, "")
    assert nas.pick_target([other, off], None)[0] is None  # none enabled
    got, why = nas.pick_target([NAS, second], None)
    assert got is None and "more than one" in why and "--name" in why
    assert nas.pick_target([NAS, second], "nas2")[0] == second
    assert "named 'x'" in nas.pick_target([NAS], "x")[1]


def test_the_command_never_mentions_or_touches_the_plaintext_key_copy() -> None:
    source = Path(nas.__file__).read_text()
    code = source.split('"""', 2)[2]  # everything after the module docstring

    assert "plaintext" not in code and "soul.key" not in code


# ---- the real restic calls, against a fake `restic` on PATH -----------------------------------

_FAKE_RESTIC = '''#!{python}
import json, os, sys
state = json.load(open(os.environ["FAKE_RESTIC_STATE"]))
args = sys.argv[1:]
cmd = args[0] if args else ""
if cmd == "list":
    sys.exit(0)  # no locks
if cmd == "snapshots":
    out = state.get("stdout", "")
    if out:
        sys.stdout.write(out)
    if state.get("stderr"):
        print(state["stderr"], file=sys.stderr)
    sys.exit(state.get("exit", 0))
if cmd == "init":
    print(state.get("init_stderr", ""), file=sys.stderr)
    sys.exit(state.get("init_exit", 0))
sys.exit(0)
'''


@pytest.fixture
def fake_restic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    import json
    import os
    import stat
    import sys

    state_path = tmp_path / "state.json"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    exe = bin_dir / "restic"
    exe.write_text(_FAKE_RESTIC.format(python=sys.executable))
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_RESTIC_STATE", str(state_path))

    def configure(**state: Any) -> None:
        state_path.write_text(json.dumps(state))

    configure()
    return configure


def _snaps(*rows: dict[str, Any]) -> str:
    import json

    return json.dumps(list(rows))


def _row(short: str, when: str, paths: list[str] | None = None) -> dict[str, Any]:
    return {"id": short + "f" * 56, "short_id": short, "time": when,
            "paths": paths or ["/home/u/osiris-vault"]}


@pytest.mark.parametrize("text,expected", [
    ("2026-11-13T11:05:00.123456789-06:00", datetime(2026, 11, 13, 17, 5, 0, 123456, tzinfo=UTC)),
    ("2026-11-13T17:05:00Z", datetime(2026, 11, 13, 17, 5, 0, tzinfo=UTC)),
    ("2026-11-13T17:05:00+00:00", datetime(2026, 11, 13, 17, 5, 0, tzinfo=UTC)),
    ("2026-11-13T17:05:00", datetime(2026, 11, 13, 17, 5, 0, tzinfo=UTC)),
    ("not a time", None), ("", None), (None, None),
])
def test_snapshot_times_are_read_as_instants(text: Any, expected: datetime | None) -> None:
    assert nas.parse_time(text) == expected


def test_the_newest_snapshot_is_found_out_of_order_and_across_time_zones(
    fake_restic: Any,
) -> None:
    # as strings these sort the other way round: 12:00-06:00 is 18:00 UTC, 14:00Z is earlier
    fake_restic(stdout=_snaps(
        _row("aaaaaaaa", "2026-11-13T14:00:00Z"),
        _row("bbbbbbbb", "2026-11-13T12:00:00.5-06:00"),   # 18:00 UTC: the newest
        _row("cccccccc", "2026-11-13T09:00:00+00:00")))

    got = nas._latest_snapshot("sftp:nas:/r", b"pw")

    assert got is not None and got["id"] == "bbbbbbbb"
    assert got["time"].startswith("2026-11-13T18:00:00")
    assert got["paths"] == ["/home/u/osiris-vault"]


def test_an_empty_repository_has_no_latest_snapshot(fake_restic: Any) -> None:
    fake_restic(stdout="[]")
    assert nas._latest_snapshot("sftp:nas:/r", b"pw") is None


def test_malformed_snapshot_output_is_no_snapshot_not_a_crash(fake_restic: Any) -> None:
    fake_restic(stdout="{this is not json")
    assert nas._latest_snapshot("sftp:nas:/r", b"pw") is None
    fake_restic(stdout='[{"id": "x", "short_id": "xxxxxxxx", "time": "garbage"}]')
    assert nas._latest_snapshot("sftp:nas:/r", b"pw") is None
    fake_restic(stdout='["a string", 3, null]')
    assert nas._latest_snapshot("sftp:nas:/r", b"pw") is None


def test_a_failing_snapshot_listing_is_no_snapshot(fake_restic: Any) -> None:
    fake_restic(stdout=_snaps(_row("aaaaaaaa", "2026-11-13T14:00:00Z")), exit=1)
    assert nas._latest_snapshot("sftp:nas:/r", b"pw") is None


async def test_a_snapshot_with_a_nanosecond_local_time_passes_the_freshness_check(
    fake_restic: Any,
) -> None:
    fresh = (NOW - timedelta(minutes=3)).astimezone(timezone_minus_six()).isoformat()
    fake_restic(stdout=_snaps(_row("ab12cd34", fresh)))
    world = World()
    real = nas._latest_snapshot("sftp:nas:/r", b"pw")
    assert real is not None
    world.snapshot = real

    rc, lines = await _run(world)

    assert rc == 0 and any("ab12cd34" in ln for ln in lines)


def timezone_minus_six() -> Any:
    from datetime import timezone

    return timezone(timedelta(hours=-6))


@pytest.mark.parametrize("state,expect", [
    ({"stdout": "[]", "exit": 0}, ("ok", 0)),
    ({"stdout": '[{"id": "x"}, {"id": "y"}]', "exit": 0}, ("ok", 2)),
    ({"stdout": "garbage", "exit": 0}, ("ok", 0)),
    ({"stderr": "Fatal: unable to open config file: no such file or directory", "exit": 10},
     ("missing", "")),
    ({"stderr": "Fatal: the repository wording changed", "exit": 10}, ("missing", "")),
    ({"stderr": "Fatal: wrong password or no key found", "exit": 12}, ("error", None)),
    ({"stderr": "Fatal: unable to create lock: repository is already locked exclusively by "
                "PID 7 on box", "exit": 11}, ("busy", None)),
    ({"stderr": "Fatal: unable to open config file: ssh: connect to host truenas port 22: "
                "No route to host", "exit": 1}, ("unreachable", None)),
])
def test_the_repository_state_follows_restics_exit_codes(
    fake_restic: Any, state: dict[str, Any], expect: tuple[str, Any],
) -> None:
    fake_restic(**state)

    kind, info = nas._repo_state("sftp:nas:/r", b"pw")

    assert kind == expect[0]
    if expect[1] is not None:
        assert info == expect[1]
    else:
        assert isinstance(info, str) and info  # a reason is always given


def test_a_wrong_password_never_reads_as_a_missing_repository(fake_restic: Any) -> None:
    # exit 12 with text that says "does not exist" must still not allow a first-time create
    fake_restic(stderr="Fatal: repository does not exist", exit=12)

    kind, _ = nas._repo_state("sftp:nas:/r", b"pw")

    assert kind == "error"


def test_creating_a_repository_reports_restics_own_failure(fake_restic: Any) -> None:
    fake_restic(init_exit=1, init_stderr="Fatal: create repository failed: permission denied")
    assert "permission denied" in (nas._init_repo("sftp:nas:/r", b"pw") or "")

    fake_restic(init_exit=0)
    assert nas._init_repo("sftp:nas:/r", b"pw") is None


async def test_check_prints_the_exact_command_to_run_next() -> None:
    world = World()

    rc, lines = await _run(world, check_only=True)

    assert rc == 0
    assert lines[-1] == "to do the offload, run: osiris nas-first-offload --name nas"
