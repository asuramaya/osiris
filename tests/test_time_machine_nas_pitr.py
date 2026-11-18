"""A point in time held only on the NAS: the WAL cover, the pull, and the status text.
The pull is tested against a REAL local restic repository whose two snapshots hold different
slices of the log, which is the shape of the real repository."""
from __future__ import annotations

import contextlib
import io
import os
import shutil
import subprocess
import tarfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import time_machine as tm
from src.orchestrator import time_machine_pitr as pitr
from src.orchestrator import time_machine_restic as tr

from tests.test_time_machine_pitr import FakeDocker

BASE = "osiris-basebackup-20261018-205720.tar.gz"
HOME = {"present": True, "reason": None, "dev": "eth0"}
T0 = datetime(2026, 10, 18, 21, 0, tzinfo=UTC)       # when the base backup finished


def _ref(pos: int) -> tr.SegRef:
    return tr.SegRef(pos, tr.seg_name(1, pos), f"/v/wal_archive/{tr.seg_name(1, pos)}",
                     T0 + timedelta(hours=pos - 2), 1000)


def test_segment_names_round_trip() -> None:
    assert tr.seg_key("000000010000000000000003.zst") == (1, 3)
    assert tr.seg_key("0000000100000107000000BE") == (1, 0x107 * 256 + 0xBE)
    assert tr.seg_key("000000010000000000000003.00000028.backup") is None
    assert tr.seg_name(1, 0x107 * 256 + 0xBE) == "0000000100000107000000BE.zst"


def _slices(spec: dict[str, tuple[int, int]]) -> Any:
    calls: list[str] = []

    def segments_in(sid: str) -> dict[int, tr.SegRef]:
        calls.append(sid)
        lo, hi = spec[sid]
        return {p: _ref(p) for p in range(lo, hi + 1)}

    segments_in.calls = calls  # type: ignore[attr-defined]
    return segments_in


SNAPS = [(T0 + timedelta(hours=6, minutes=10), "A"),    # holds 3..8   (older slice)
         (T0 + timedelta(hours=10, minutes=10), "B")]   # holds 6..12  (3..5 thinned away)


def test_the_cover_takes_each_missing_stretch_from_a_snapshot_that_holds_it() -> None:
    seg = _slices({"A": (3, 8), "B": (6, 12)})
    # 30 minutes before segment 9 was stamped: the first stamped after is 9, and two more copy
    # batches (10, 11) are taken past it
    target = _ref(9).mtime - timedelta(minutes=30)
    cover = tr.plan_wal_cover(snapshots=SNAPS, target=target, tl=1, first_pos=3,
                              segments_in=seg)
    assert cover.last_pos == 11 and cover.first_pos == 3
    got = {p.pos for _sid, refs in cover.chosen for p in refs}
    assert got == set(range(3, 12))
    by = {sid: sorted(r.pos for r in refs) for sid, refs in cover.chosen}
    assert by == {"B": [6, 7, 8, 9, 10, 11], "A": [3, 4, 5]}   # no segment taken twice
    assert cover.segments == 9 and cover.bytes == 9000


def test_the_log_is_taken_through_two_more_copy_batches_past_the_first_stamped() -> None:
    """Segments carry the time the vault COPIED them, in batches: the one that holds the picked
    time can be several batches after the first segment stamped after it."""
    def batch(pos: int) -> tr.SegRef:                 # three segments per copy run, 1 s apart
        run, within = divmod(pos, 3)
        return tr.SegRef(pos, tr.seg_name(1, pos), f"/v/wal_archive/{tr.seg_name(1, pos)}",
                         T0 + timedelta(hours=run * 3) + timedelta(seconds=within), 1000)

    segs = {p: batch(p) for p in range(3, 40)}
    target = batch(10).mtime - timedelta(minutes=1)    # first stamped after: segment 10
    cover = tr.plan_wal_cover(snapshots=[(T0 + timedelta(days=9), "A")], target=target, tl=1,
                              first_pos=3, segments_in=lambda _s: segs)
    # segment 10 is in the batch of 9..11; the next two batches are 12..14 and 15..17
    assert cover.last_pos == 17


def test_the_top_snapshot_is_one_made_well_after_the_time() -> None:
    seg = _slices({"A": (3, 12), "B": (3, 12)})
    snaps = [(T0 + timedelta(hours=7), "A"), (T0 + timedelta(hours=30), "B")]
    tr.plan_wal_cover(snapshots=snaps, target=_ref(6).mtime, tl=1, first_pos=3,
                      segments_in=seg)
    assert seg.calls[0] == "B"          # not "A", which was made only just after the time


def test_a_short_run_takes_what_the_snapshot_has() -> None:
    seg = _slices({"A": (3, 8), "B": (6, 12)})
    cover = tr.plan_wal_cover(snapshots=SNAPS, target=_ref(11).mtime - timedelta(minutes=1),
                              tl=1, first_pos=3, segments_in=seg)
    assert cover.last_pos == 12          # the snapshot ends there


def test_a_hole_in_every_snapshot_refuses_and_names_the_segment() -> None:
    seg = _slices({"A": (3, 4), "B": (6, 12)})                 # segment 5 is nowhere
    with pytest.raises(tm.TimeMachineError) as err:
        tr.plan_wal_cover(snapshots=SNAPS, target=_ref(9).mtime - timedelta(minutes=30),
                          tl=1, first_pos=3, segments_in=seg)
    assert tr.seg_name(1, 5) in str(err.value) and "cannot be opened" in str(err.value)


def test_no_log_at_or_after_the_time_is_a_sentence() -> None:
    seg = _slices({"A": (3, 8), "B": (3, 8)})
    with pytest.raises(tm.TimeMachineError, match="No log was found"):
        tr.plan_wal_cover(snapshots=SNAPS, target=_ref(20).mtime, tl=1, first_pos=3,
                          segments_in=seg)
    with pytest.raises(tm.TimeMachineError, match="no snapshots"):
        tr.plan_wal_cover(snapshots=[], target=T0, tl=1, first_pos=3, segments_in=seg)


def test_the_probe_count_is_bounded() -> None:
    snaps = [(T0 + timedelta(hours=i), f"S{i}") for i in range(200)]
    seen: list[str] = []

    def seg(sid: str) -> dict[int, tr.SegRef]:
        seen.append(sid)
        return {p: _ref(p) for p in (10, 11)}                    # never holds 3..9

    with pytest.raises(tm.TimeMachineError):
        tr.plan_wal_cover(snapshots=snaps, target=_ref(10).mtime - timedelta(minutes=5),
                          tl=1, first_pos=3, segments_in=seg, max_probes=12)
    assert len(seen) <= 12


def _base_tar(path: Path, start_file: str = "000000010000000000000003") -> None:
    label = (f"START WAL LOCATION: 0/3000028 (file {start_file})\n"
             "CHECKPOINT LOCATION: 0/3000060\n").encode()
    with tarfile.open(path, "w:gz") as tf:
        info = tarfile.TarInfo("backup_label")
        info.size = len(label)
        tf.addfile(info, io.BytesIO(label))
        other = b"x" * 5000
        info2 = tarfile.TarInfo("PG_VERSION")
        info2.size = len(other)
        tf.addfile(info2, io.BytesIO(other))


def test_the_start_segment_comes_from_the_backup_label(tmp_path: Path) -> None:
    _base_tar(tmp_path / "b.tar.gz")
    assert tr.backup_label_start(tmp_path / "b.tar.gz") == (1, 3)
    (tmp_path / "bad.tar.gz").write_bytes(b"not a tarball")
    with pytest.raises(tm.TimeMachineError, match="label could not be read"):
        tr.backup_label_start(tmp_path / "bad.tar.gz")


@pytest.mark.skipif(shutil.which("restic") is None, reason="restic is not installed")
class TestRealRepository:
    @pytest.fixture
    def repo(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
        vault = tmp_path / "osiris-vault"
        (vault / "basebackups").mkdir(parents=True)
        (vault / "wal_archive").mkdir()
        url = str(tmp_path / "repo")
        env = {**os.environ, "RESTIC_REPOSITORY": url, "RESTIC_PASSWORD": "pw",
               "RESTIC_CACHE_DIR": str(tmp_path / "cache")}

        def restic(*args: str) -> None:
            subprocess.run(["restic", *args], env=env, check=True, capture_output=True,
                           timeout=120)

        restic("init")
        base = vault / "basebackups" / BASE
        _base_tar(base)
        os.utime(base, (T0.timestamp(),) * 2)

        def seg(p: int) -> None:
            f = vault / "wal_archive" / tr.seg_name(1, p)
            f.write_bytes(b"w" * 1000)
            os.utime(f, (_ref(p).mtime.timestamp(),) * 2)

        for p in range(3, 9):
            seg(p)
        restic("backup", "--time", "2026-10-19 03:10:00", str(vault))      # A: 3..8
        for p in range(3, 6):
            (vault / "wal_archive" / tr.seg_name(1, p)).unlink()           # thinned away
        for p in range(9, 13):
            seg(p)
        restic("backup", "--time", "2026-10-19 07:10:00", str(vault))      # B: 6..12
        monkeypatch.setenv("OSIRIS_TM_SCRATCH", str(tmp_path / "scratch"))
        (tmp_path / "scratch").mkdir()
        return {"env": env, "target": {"name": "nas", "path_or_url": url},
                "scratch": tmp_path / "scratch"}

    def _deps(self, repo: dict[str, Any], *, presence: dict[str, Any] | None = None,
              lock: bool = True) -> tr.Deps:
        from src.orchestrator import restic_locks

        @contextlib.contextmanager
        def hold() -> Iterator[bool]:
            yield lock

        def run(args: list[str], *, timeout: float, text: bool = True) -> Any:
            return restic_locks.run_restic(args, repo["env"], timeout=timeout, text=text)

        return tr.Deps(run=run, presence=lambda _t: presence or HOME, hold_lock=hold)

    def _moment(self, at: datetime) -> tm.Moment:
        return tm.Moment(
            id="pitr-" + BASE.removesuffix(".tar.gz"), kind="pitr", label="x",
            when=at.isoformat(), path="", size_bytes=pitr.NAS_BASE_ESTIMATE, at=at.isoformat(),
            window_from=T0.isoformat(), window_to=(T0 + timedelta(days=9)).isoformat(),
            repo_path=BASE)

    def test_the_pull_merges_slices_from_two_snapshots(self, repo: dict[str, Any],
                                                       tmp_path: Path) -> None:
        said: list[str] = []
        at = _ref(9).mtime - timedelta(minutes=30)
        base, wal = tr.pull_pitr(self._moment(at), tmp_path / "s", said.append,
                                 self._deps(repo), repo["target"], free=lambda _p: 10**12)
        assert base.name == BASE and base.stat().st_size > 0
        assert sorted(p.name for p in wal.iterdir()) == [tr.seg_name(1, p) for p in range(3, 12)]
        assert all(p.stat().st_size == 1000 for p in wal.iterdir())
        assert any("working out which snapshots" in t for t in said)
        locks = subprocess.run(["restic", "list", "locks"], env=repo["env"], text=True,
                               capture_output=True, timeout=60)
        assert locks.stdout.strip() == ""                 # reads only, no lock left behind
        assert not list((tmp_path / "s").glob("walraw-*"))

    def test_an_early_target_still_reads_the_log_from_the_newer_snapshot_too(
            self, repo: dict[str, Any], tmp_path: Path) -> None:
        at = _ref(5).mtime - timedelta(minutes=1)
        _base, wal = tr.pull_pitr(self._moment(at), tmp_path / "s", lambda _t: None,
                                  self._deps(repo), repo["target"], free=lambda _p: 10**12)
        assert sorted(p.name for p in wal.iterdir()) == [tr.seg_name(1, p) for p in range(3, 9)]

    def test_refusals_away_busy_hole_and_disk(self, repo: dict[str, Any],
                                              tmp_path: Path) -> None:
        at = _ref(9).mtime - timedelta(minutes=30)
        m = self._moment(at)
        with pytest.raises(tm.TimeMachineError, match="only be opened at home"):
            tr.pull_pitr(m, tmp_path / "a", lambda _t: None,
                         self._deps(repo, presence={"present": False}), repo["target"])
        with pytest.raises(tm.TimeMachineError, match="backup to the NAS is running"):
            tr.pull_pitr(m, tmp_path / "b", lambda _t: None, self._deps(repo, lock=False),
                         repo["target"])
        with pytest.raises(tm.TimeMachineError, match="not enough disk"):
            tr.pull_pitr(m, tmp_path / "c", lambda _t: None, self._deps(repo),
                         repo["target"], free=lambda _p: 1000)
        late = self._moment(_ref(12).mtime + timedelta(days=30))
        with pytest.raises(tm.TimeMachineError, match="No log was found"):
            tr.pull_pitr(late, tmp_path / "d", lambda _t: None, self._deps(repo),
                         repo["target"], free=lambda _p: 10**12)
        gone = tm.Moment(**{**self._moment(at).__dict__, "repo_path": "osiris-basebackup-"
                            "20250101-000000.tar.gz"})
        with pytest.raises(tm.TimeMachineError, match="no longer holds the base backup"):
            tr.pull_pitr(gone, tmp_path / "e", lambda _t: None, self._deps(repo),
                         repo["target"], free=lambda _p: 10**12)


def test_replay_progress_reads_in_words() -> None:
    text = pitr.replay_text("2026-10-30 11:02:11+00", "2026-10-30T14:00:00+00:00")
    assert text == "replaying: reached Oct 30 11:02 of Oct 30 14:00 UTC"
    assert pitr.replay_text("", None) == "" and pitr.replay_text("junk", None) == ""


def _nas_moment() -> tm.Moment:
    return tm.Moment(
        id="pitr-" + BASE.removesuffix(".tar.gz"), kind="pitr", label="x",
        when="2026-10-30T14:00:00+00:00", path="", size_bytes=pitr.NAS_BASE_ESTIMATE,
        at="2026-10-30T14:00:00+00:00", window_from="2026-10-19T02:01:38+00:00",
        window_to="2026-11-01T23:15:07+00:00", repo_path=BASE)


def test_start_pulls_shows_progress_replays_and_removes_the_scratch_copy(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_TM_SCRATCH", str(tmp_path / "scratch"))
    (tmp_path / "scratch").mkdir()
    texts: list[str] = []

    def fake_pull(moment: tm.Moment, scratch: Path, progress: Any) -> tuple[Path, Path]:
        progress("pulling base 1.0/3.8 GB")
        (scratch / "wal").mkdir(parents=True)
        base = scratch / "base.tar.gz"
        base.write_bytes(b"b")
        (scratch / "wal" / "x.zst").write_bytes(b"w")
        return base, scratch / "wal"

    d = FakeDocker()
    rec = tm.start(_nas_moment(), sh=d, record_path=tmp_path / "rec.json",
                   sleep=lambda _s: None, free=lambda _p: 10**12, pull_pitr=fake_pull,
                   on_phase=lambda r: texts.append(r.public()["state_text"]))
    assert rec.state == "ready"
    assert any("pulling base 1.0/3.8 GB" in t for t in texts)
    assert any(t.startswith("Fetching") for t in texts)
    server = next(c for c in d.calls if c[:3] == ["docker", "run", "-d"])
    assert any(str(tmp_path / "scratch") in a and a.endswith(":ro") for a in server)
    assert not list((tmp_path / "scratch").glob("osiris-tm-*"))     # removed after the replay
    assert tm.read_record(tmp_path / "rec.json").scratch == ""      # type: ignore[union-attr]


def test_a_failed_nas_pull_leaves_nothing_and_says_why(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_TM_SCRATCH", str(tmp_path / "scratch"))
    (tmp_path / "scratch").mkdir()

    def away(moment: tm.Moment, scratch: Path, progress: Any) -> tuple[Path, Path]:
        (scratch / "base").mkdir(parents=True)
        (scratch / "base" / "half").write_bytes(b"h")
        raise tm.TimeMachineError("Not on the home network.")

    d = FakeDocker()
    with pytest.raises(tm.TimeMachineError, match="home network"):
        tm.start(_nas_moment(), sh=d, record_path=tmp_path / "rec.json",
                 sleep=lambda _s: None, free=lambda _p: 10**12, pull_pitr=away)
    assert not d.containers and not d.volumes
    assert not list((tmp_path / "scratch").glob("osiris-tm-*"))
    assert tm.read_record(tmp_path / "rec.json") is None


@pytest.mark.asyncio
async def test_the_console_refuses_a_nas_window_away_from_home(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    from src.orchestrator import time_machine_console as tmc

    (tmp_path / "rp.json").write_text(json.dumps({"windows": [
        {"from": "2026-10-19T02:01:38+00:00", "to": "2026-11-01T23:15:07+00:00",
         "base": BASE}], "gaps": []}))
    monkeypatch.setenv("OSIRIS_RESTORE_POINTS_FILE", str(tmp_path / "rp.json"))
    tr._write_cache({"as_of": "x", "repo": "r", "target": {"path_or_url": "/r"}, "dumps": []})
    monkeypatch.setattr("src.orchestrator.network_presence.lan_presence",
                        lambda _t: {"present": False, "reason": "reachable only over vpn"})
    spawned: list[str] = []
    monkeypatch.setattr(tm, "spawn_run", lambda mid, **_k: spawned.append(mid) or 1)
    out = await tmc.start("pitr-" + BASE.removesuffix(".tar.gz"), "2026-10-30T14:00:00Z")
    assert "only be opened at home" in out["error"] and spawned == []
    listing = (await tmc.moments())["moments"]
    nas = [m for m in listing if m["kind"] == "pitr" and m["id"].endswith("205720")]
    assert nas and nas[0]["available"] is False and "vpn" in nas[0]["reason"]
