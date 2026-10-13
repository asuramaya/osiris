"""Retention runs daily, once per manifest, after the restore drill, and a backlog is visible.

The incident: the Saturday manifest timer fired at 09:05 because the laptop slept, so the Sunday
03:00 apply found it 17.9 hours old, refused, exited 1, and the next attempt was a week away
while archived WAL grew to 417 GB. These tests drive the real script entry point
(`main(["--apply-if-clear", ...])`) and read the real unit files."""
from __future__ import annotations

import asyncio
import io
import re
import shlex
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import scripts.osiris_prune_ladder as ladder
from scripts.osiris_prune_ladder import DumpFile, mark_base_backup_verified
from src.actions.core import Actions

ROOT = Path(__file__).resolve().parent.parent
DEPLOY = ROOT / "deploy"


@pytest.fixture(autouse=True)
def _use_test_dsn(pg_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ladder, "DSN", pg_dsn)

    async def _no_sessions() -> list[Any]:
        return []

    monkeypatch.setattr(ladder, "_collect_session_prune_plan", _no_sessions)


def _dirs(tmp_path: Path) -> tuple[Path, Path]:
    backups, vault = tmp_path / "backups", tmp_path / "vault"
    (vault / "basebackups").mkdir(parents=True)
    (vault / "wal_archive").mkdir()
    backups.mkdir()
    return backups, vault


async def _run(args: list[str]) -> int:
    """main() starts its own event loop, so it runs in a worker thread beside the test's."""
    return await asyncio.to_thread(ladder.main, args)


def _args(backups: Path, vault: Path) -> list[str]:
    return ["--apply-if-clear", "--backups", str(backups), "--vault", str(vault)]


async def _send_manifest(actions: Actions, *, hours_old: float) -> int:
    empty: dict[str, list[DumpFile]] = {"keep": [], "remove": []}
    plans = {"backups/": empty, "vault": empty, "vault/basebackups": empty}
    mid = await ladder.mail_manifest(plans, {"keep": [], "remove": []})
    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at = now() - make_interval(secs => $2) WHERE id=$1",
        mid, hours_old * 3600)
    return mid


def _old_cache_dump(backups: Path, vault: Path, name: str) -> Path:
    """A day-old-plus dump in backups/ that the vault also holds: planned for removal."""
    cached = backups / name
    cached.write_bytes(b"x" * 10)
    (vault / name).write_bytes(b"x" * 10)
    return cached


# --- the incident, through the real entry point --------------------------------------------


async def test_a_manifest_too_young_is_a_quiet_skip_and_the_next_day_applies_it(
    actions: Actions, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    backups, vault = _dirs(tmp_path)
    cached = _old_cache_dump(backups, vault, "osiris-20200101-000000.dump")
    mid = await _send_manifest(actions, hours_old=17.9)  # Sunday 03:00 after a late Saturday

    assert await _run(_args(backups, vault)) == 0  # not a failure
    out = capsys.readouterr()
    assert "SKIPPING" in out.out and "not yet old enough" in out.out
    assert cached.exists()

    await actions.pool.execute(
        "UPDATE fleet_messages SET created_at = now() - interval '26 hours' WHERE id=$1", mid)
    assert await _run(_args(backups, vault)) == 0  # the next daily run makes it up
    assert not cached.exists()
    assert ladder.last_applied_manifest() == mid


async def test_the_same_manifest_is_applied_once_not_every_day(
    actions: Actions, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    backups, vault = _dirs(tmp_path)
    await _send_manifest(actions, hours_old=30)
    first = _old_cache_dump(backups, vault, "osiris-20200101-000000.dump")
    assert await _run(_args(backups, vault)) == 0
    assert not first.exists()

    second = _old_cache_dump(backups, vault, "osiris-20200102-000000.dump")
    assert await _run(_args(backups, vault)) == 0
    assert "already applied" in capsys.readouterr().out
    assert second.exists()  # a daily timer must not re-apply yesterday's manifest


async def test_a_newer_manifest_is_applied_again(actions: Actions, tmp_path: Path) -> None:
    backups, vault = _dirs(tmp_path)
    await _send_manifest(actions, hours_old=200)
    assert await _run(_args(backups, vault)) == 0
    newer = await _send_manifest(actions, hours_old=25)
    later = _old_cache_dump(backups, vault, "osiris-20200102-000000.dump")

    assert await _run(_args(backups, vault)) == 0

    assert not later.exists()
    assert ladder.last_applied_manifest() == newer


async def test_no_manifest_or_a_dimmed_one_is_a_quiet_skip_that_deletes_nothing(
    actions: Actions, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    from src.orchestrator.mailbox import dim_brief

    backups, vault = _dirs(tmp_path)
    cached = _old_cache_dump(backups, vault, "osiris-20200101-000000.dump")
    assert await _run(_args(backups, vault)) == 0  # none ever sent
    assert "no manifest" in capsys.readouterr().out

    mid = await _send_manifest(actions, hours_old=30)
    await dim_brief(actions.pool, mid, because="plan changed", by="test:agent")
    assert await _run(_args(backups, vault)) == 0
    assert "dimmed" in capsys.readouterr().out
    assert cached.exists()
    assert ladder.last_applied_manifest() is None


# --- the unit files: daily, after the drill, chained on its success -----------------------


def _exec_args(line: str) -> list[str]:
    """The arguments after the script path of one ExecStart / ExecStartPost line."""
    tokens = shlex.split(line.split("=", 1)[1].lstrip("-"))
    return tokens[tokens.index("scripts/osiris_prune_ladder.py") + 1:]


def test_the_apply_timer_runs_daily_and_catches_up_after_sleep() -> None:
    timer = (DEPLOY / "osiris-prune-apply.timer").read_text()
    assert re.search(r"^OnCalendar=\*-\*-\* \d\d:\d\d:\d\d$", timer, re.M)
    assert "Persistent=true" in timer
    assert "Sun" not in _match("osiris-prune-apply.timer", r"^OnCalendar=.*$").group(0)


def _match(path: str, pattern: str) -> re.Match[str]:
    found = re.search(pattern, (DEPLOY / path).read_text(), re.M)
    assert found, f"{path}: {pattern}"
    return found


def test_the_apply_runs_after_the_drills_six_hour_stop_on_sunday() -> None:
    hh, mm = map(int, _match("osiris-prune-apply.timer",
                             r"^OnCalendar=\*-\*-\* (\d\d):(\d\d)").groups())
    dh, dm = map(int, _match("osiris-pitr-drill.timer",
                             r"^OnCalendar=Sun \*-\*-\* (\d\d):(\d\d)").groups())
    stop = int(_match("osiris-pitr-drill.service", r"^TimeoutStartSec=(\d+)h").group(1))
    assert (hh * 60 + mm) > (dh * 60 + dm + stop * 60)
    assert "After=osiris-pitr-drill.service" in (DEPLOY / "osiris-prune-apply.service").read_text()


def test_the_drill_applies_retention_only_after_it_passed() -> None:
    service = (DEPLOY / "osiris-pitr-drill.service").read_text()
    start = service.index("\nExecStart=")
    post = service.index("\nExecStartPost=")
    assert post > start
    post_line = service[post + 1:].splitlines()[0]
    assert post_line.startswith("ExecStartPost=-")  # a failing apply never fails a passed drill
    apply_line = (DEPLOY / "osiris-prune-apply.service").read_text()
    exec_line = next(line for line in apply_line.splitlines() if line.startswith("ExecStart="))
    assert _exec_args(post_line) == _exec_args(exec_line)


async def test_the_units_exact_arguments_run_the_real_entry_point(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    backups, vault = _dirs(tmp_path)
    cached = _old_cache_dump(backups, vault, "osiris-20200101-000000.dump")
    await _send_manifest(actions, hours_old=30)
    monkeypatch.setenv("HOME", str(tmp_path))
    exec_line = next(line for line in (DEPLOY / "osiris-prune-apply.service").read_text()
                     .splitlines() if line.startswith("ExecStart="))
    args = [a.replace("%h/code/osiris/backups", str(backups)).replace("%h/osiris-vault",
                                                                         str(vault))
            for a in _exec_args(exec_line)]

    assert await _run(args) == 0

    assert not cached.exists()


# --- readiness: archived WAL older than the newest verified base --------------------------


def _verified_base(vault: Path, start: str) -> Path:
    base = vault / "basebackups" / "osiris-basebackup-20261012-020000.tar.gz"
    label = f"START WAL LOCATION: 1/0 (file {start})\n".encode()
    with tarfile.open(base, "w:gz") as tf:
        info = tarfile.TarInfo("backup_label")
        info.size = len(label)
        tf.addfile(info, io.BytesIO(label))
    mark_base_backup_verified(base, now=datetime.now(UTC))
    return base


def test_the_backlog_counts_wal_before_the_verified_bases_start_and_nothing_else(
    tmp_path: Path,
) -> None:
    from src.orchestrator import wal_backlog

    _, vault = _dirs(tmp_path)
    _verified_base(vault, "000000010000000000000005")
    # segment names are 24 hex digits: timeline 1, log 0, segment n
    for n, size in ((3, 1000), (4, 2000), (5, 4000), (6, 8000)):
        (vault / "wal_archive" / f"00000001{0:08X}{n:08X}").write_bytes(b"x" * size)

    wal_backlog.clear_cache()
    out = wal_backlog.wal_backlog(vault)

    assert out["verified"] is True
    assert out["segments"] == 2 and out["bytes"] == 3000  # segments 3 and 4 only


def test_with_no_verified_base_backup_nothing_can_be_judged(tmp_path: Path) -> None:
    from src.orchestrator import wal_backlog

    _, vault = _dirs(tmp_path)
    (vault / "wal_archive" / f"00000001{0:08X}{1:08X}").write_bytes(b"x" * 100)
    wal_backlog.clear_cache()
    assert wal_backlog.wal_backlog(vault)["verified"] is None


def test_readiness_flags_a_backlog_over_the_limit_and_not_under_it() -> None:
    from src.orchestrator.readiness import compute_readiness_steps

    def step(backlog: dict[str, Any] | None, limit: int = 50) -> dict[str, Any]:
        steps = compute_readiness_steps(
            soul_key={"present": False}, restic_key={"present": False}, offload_targets=[],
            restore_drill_receipts={}, services_restarted=None, wal_backlog=backlog,
            wal_limit_gb=limit)
        return next(s for s in steps if s["key"] == "wal_backlog")

    over = step({"verified": True, "bytes": 417 * 10**9, "segments": 25000})
    assert over["status"] == "needs_attention"
    assert "417 GB" in over["reason"] and "limit 50 GB" in over["reason"]
    assert step({"verified": True, "bytes": 30 * 10**9, "segments": 10})["status"] == "done"
    off = step({"verified": True, "bytes": 417 * 10**9, "segments": 1}, limit=0)
    assert off["status"] == "done"
    unknown = step({"verified": None, "bytes": 0, "segments": 0})
    assert unknown["status"] == "missing" and unknown["optional"] is True
    assert step(None)["status"] == "missing"


def test_the_backlog_limit_is_a_registered_setting_defaulting_to_fifty() -> None:
    from src.config.settings import Settings
    from src.config.settings_registry import spec_by_key

    assert spec_by_key("backup.wal_backlog_gb").default == 50
    assert Settings().osiris_wal_backlog_gb == 50
