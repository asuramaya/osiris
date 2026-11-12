"""The receipts file is what says a target has offloaded before, so it is written atomically and
a file that does not parse can never read as 'never offloaded' (which is what lets a first-time
`restic init` through)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import offload_runner
from src.orchestrator.backup_settings import write_backup_settings


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(offload_runner._RECEIPTS_ENV, str(tmp_path / "offload_receipts.json"))
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "a-test-password")


def test_a_write_that_dies_midway_leaves_the_old_receipts_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    offload_runner._write_receipt("nas", {"last_successful_offload": "2026-11-01T00:00:00+00:00"})
    before = (tmp_path / "offload_receipts.json").read_text()

    def dies(self: Path, data: str, *a: Any, **kw: Any) -> int:
        self.open("w").write(data[: len(data) // 2])  # half a file, then the process dies
        raise KeyboardInterrupt

    monkeypatch.setattr(Path, "write_text", dies)
    with pytest.raises(KeyboardInterrupt):
        offload_runner._write_receipt("nas", {"last_error": "x"})
    monkeypatch.undo()

    assert (tmp_path / "offload_receipts.json").read_text() == before
    assert not list(tmp_path.glob("*.tmp"))  # no stray temp file
    assert json.loads(before)["nas"]["last_successful_offload"]


def test_a_receipts_file_that_does_not_parse_reads_as_untrusted(tmp_path: Path) -> None:
    (tmp_path / "offload_receipts.json").write_text('{"nas": {"last_succ')  # cut off

    receipts, untrusted = offload_runner._load_receipts()

    assert receipts == {} and untrusted is True


def test_a_missing_receipts_file_is_a_clean_first_run() -> None:
    assert offload_runner._load_receipts() == ({}, False)


def test_a_write_over_an_unparseable_file_sets_it_aside_and_keeps_the_mark(
    tmp_path: Path,
) -> None:
    path = tmp_path / "offload_receipts.json"
    path.write_text("not json")

    offload_runner._write_receipt("nas", {"last_attempt_at": "now"})

    assert (tmp_path / "offload_receipts.json.unreadable").read_text() == "not json"
    assert json.loads(path.read_text())["nas"]["last_attempt_at"] == "now"
    _, untrusted = offload_runner._load_receipts()
    assert untrusted is True  # stays until the operator removes the .unreadable copy
    (tmp_path / "offload_receipts.json.unreadable").unlink()
    assert offload_runner._load_receipts()[1] is False


async def _one_target(actions: Actions) -> None:
    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic", "path_or_url": "sftp:nas:/r",
                          "schedule": "*-*-* 03:00:00", "enabled": True}])


async def test_the_tick_never_allows_init_when_the_receipts_did_not_parse(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    (tmp_path / "offload_receipts.json").write_text("{broken")
    seen: list[bool] = []
    monkeypatch.setattr(offload_runner, "_run_restic_backup",
                        lambda **kw: seen.append(kw["init_if_needed"]))

    await offload_runner.run_offload_tick(actions.pool)

    assert seen == [False]


async def test_the_refusal_names_the_marker_and_the_remedy_not_a_false_history(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    (tmp_path / "offload_receipts.json").write_text("{broken")
    seen: list[str | None] = []
    monkeypatch.setattr(offload_runner, "_run_restic_backup",
                        lambda **kw: seen.append(kw["no_init_reason"]))

    await offload_runner.run_offload_tick(actions.pool)

    reason = seen[0] or ""
    assert "offload_receipts.json.unreadable" in reason and "restic init" in reason
    assert "cannot tell whether nas has offloaded before" in reason


def test_the_backup_returns_the_given_reason_instead_of_claiming_an_offload_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os
    import stat
    import sys

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    exe = bin_dir / "restic"
    exe.write_text(f"#!{sys.executable}\nimport sys\n"
                   "print('Fatal: repository does not exist', file=sys.stderr)\nsys.exit(10)\n")
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    out = offload_runner._run_restic_backup(
        repository="fake", password=b"x", source=tmp_path, init_if_needed=False,
        no_init_reason="because the receipts are damaged")

    assert out == "because the receipts are damaged"


async def test_the_tick_allows_init_for_a_target_with_no_receipts_at_all(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    seen: list[bool] = []
    monkeypatch.setattr(offload_runner, "_run_restic_backup",
                        lambda **kw: seen.append(kw["init_if_needed"]))

    await offload_runner.run_offload_tick(actions.pool)

    assert seen == [True]


async def test_an_incomplete_snapshot_note_reaches_the_receipt(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)

    def backup(**kw: Any) -> None:
        kw["notes"].append("snapshot saved, but restic could not read some source files: x")

    monkeypatch.setattr(offload_runner, "_run_restic_backup", backup)

    out = await offload_runner.run_offload_tick(actions.pool)

    assert out["targets"][0]["ok"] is True and "could not read" in out["targets"][0]["note"]
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["last_error"] is None and "could not read" in receipt["last_note"]
