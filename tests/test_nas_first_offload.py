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
    assert lines[-1].startswith("check only")


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
