"""The Time Machine throwaway lifecycle, against a fake docker: refusals, limits, cleanup."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from src.orchestrator import time_machine as tm


class FakeDocker:
    def __init__(self, *, live_volumes: str = "livevol", fail_on: str | None = None,
                 objects: str = "42") -> None:
        self.calls: list[list[str]] = []
        self.containers: set[str] = set()
        self.volumes: set[str] = set()
        self.live_volumes = live_volumes
        self.fail_on = fail_on
        self.objects = objects

    def __call__(self, cmd: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
        self.calls.append(cmd)
        joined = " ".join(cmd)
        if self.fail_on and self.fail_on in joined:
            return subprocess.CompletedProcess(cmd, 1, "", "boom")
        ok = subprocess.CompletedProcess(cmd, 0, "", "")
        if cmd[:3] == ["docker", "info", "--format"]:
            return subprocess.CompletedProcess(cmd, 0, "/var/lib/docker\n", "")
        if cmd[:2] == ["docker", "inspect"]:
            if "Mounts" in joined:
                return subprocess.CompletedProcess(cmd, 0, self.live_volumes + " \n", "")
            return subprocess.CompletedProcess(cmd, 0, "osiris-pg:16-repack\n", "")
        if cmd[:3] == ["docker", "volume", "create"]:
            self.volumes.add(cmd[-1])
        elif cmd[:3] == ["docker", "volume", "rm"]:
            self.volumes.discard(cmd[-1])
        elif cmd[:3] == ["docker", "volume", "ls"]:
            return subprocess.CompletedProcess(cmd, 0, "\n".join(self.volumes), "")
        elif cmd[:3] == ["docker", "run", "-d"]:
            self.containers.add(cmd[cmd.index("--name") + 1])
        elif cmd[:3] == ["docker", "rm", "-f"]:
            self.containers.discard(cmd[-1])
        elif cmd[:3] == ["docker", "ps", "-a"]:
            return subprocess.CompletedProcess(cmd, 0, "\n".join(self.containers), "")
        elif "SELECT count(*)" in joined:
            return subprocess.CompletedProcess(cmd, 0, self.objects + "\n", "")
        return ok


@pytest.fixture
def dump(tmp_path: Path) -> tm.Moment:
    p = tmp_path / "osiris-20261115-201856.dump"
    p.write_bytes(b"x" * 1000)
    return tm.Moment("dump", "dump", "2026-11-15T20:18:56+00:00", str(p), 1000)


def go(dump: tm.Moment, docker: FakeDocker, tmp_path: Path, **kw: object) -> tm.Throwaway:
    return tm.start(dump, sh=docker, record_path=tmp_path / "rec.json",
                    sleep=lambda _s: None, free=lambda _p: 10**12, **kw)  # type: ignore[arg-type]


def test_moments_newest_first_and_skip_junk(tmp_path: Path) -> None:
    (tmp_path / "osiris-20261113-043006.dump").write_bytes(b"a")
    (tmp_path / "osiris-20261115-201856.dump").write_bytes(b"b")
    (tmp_path / "osiris-20261116-100434.dump").write_bytes(b"")          # half-written
    (tmp_path / "osiris-20261114-085137.dump.tmp").write_bytes(b"c")     # not a dump
    (tmp_path / "notes.txt").write_bytes(b"d")
    got = [Path(m.path).name for m in tm.list_moments(tmp_path)]
    assert got == ["osiris-20261115-201856.dump", "osiris-20261113-043006.dump"]


def test_start_makes_a_labelled_local_only_throwaway(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()
    rec = go(dump, d, tmp_path)
    assert rec.state == "ready"
    run_cmd = next(c for c in d.calls if c[:3] == ["docker", "run", "-d"])
    assert f"{tm.LABEL}=1" in run_cmd
    pub = run_cmd[run_cmd.index("-p") + 1]
    assert pub.startswith("127.0.0.1:") and pub.endswith(":5432")
    assert int(pub.split(":")[1]) not in tm.LIVE_PORTS
    assert "livevol" not in " ".join(run_cmd)
    assert rec.container in d.containers and rec.volume in d.volumes
    assert any("/tm/restore.dump:ro" in a for a in run_cmd)
    # the record on disk carries the passwords, the public view never does
    assert tm.read_record(tmp_path / "rec.json") is not None
    pub_view = rec.public()
    assert "reader_password" not in pub_view and "superuser_password" not in pub_view
    assert rec.reader_dsn().startswith("postgresql://tm_reader:")


def test_reader_role_cannot_write() -> None:
    sql = tm.reader_role_sql("abcDEF_123-x")
    assert "default_transaction_read_only = on" in sql
    assert "statement_timeout" in sql
    assert "GRANT SELECT" in sql and "INSERT" not in sql and "ALL PRIVILEGES" not in sql
    with pytest.raises(tm.TimeMachineError):
        tm.reader_role_sql("x'; DROP TABLE objects; --")


def test_second_start_is_refused_while_one_exists(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()
    go(dump, d, tmp_path)
    with pytest.raises(tm.TimeMachineError, match="already"):
        go(dump, d, tmp_path)
    assert len(d.containers) == 1


def test_disk_check_refuses_before_creating_anything(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()
    with pytest.raises(tm.TimeMachineError, match="not enough disk"):
        tm.start(dump, sh=d, record_path=tmp_path / "rec.json", free=lambda _p: 2500)
    assert not d.containers and not d.volumes
    assert tm.read_record(tmp_path / "rec.json") is None
    assert tm.disk_problem(1000, 3000) is None
    assert tm.disk_problem(1000, 2999) is not None


@pytest.mark.parametrize("step", ["volume create", "run -d", "pg_isready", "pg_restore",
                                  "SELECT count(*)", "CREATE ROLE"])
def test_any_failure_tears_everything_down(dump: tm.Moment, tmp_path: Path, step: str) -> None:
    d = FakeDocker(fail_on=step)
    t = tm
    t_sleep_calls = {"n": 0}

    def sleep(_s: float) -> None:
        t_sleep_calls["n"] += 1

    clock = iter(range(0, 10**6, 30))
    with pytest.raises(tm.TimeMachineError):
        tm.start(dump, sh=d, record_path=tmp_path / "rec.json", sleep=sleep,
                 free=lambda _p: 10**12, now=lambda: float(next(clock)))
    assert not d.containers and not d.volumes
    assert t.read_record(tmp_path / "rec.json") is None


def test_an_empty_restore_is_a_failure(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker(objects="0")
    with pytest.raises(tm.TimeMachineError, match="no objects"):
        go(dump, d, tmp_path)
    assert not d.containers and not d.volumes


def test_timeout_is_reported_and_cleaned(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()

    def slow(cmd: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
        if "pg_restore" in cmd:
            raise subprocess.TimeoutExpired(cmd, timeout)
        return d(cmd, timeout)

    with pytest.raises(tm.TimeMachineError, match="timed out"):
        tm.start(dump, sh=slow, record_path=tmp_path / "rec.json",
                 sleep=lambda _s: None, free=lambda _p: 10**12)
    assert not d.containers and not d.volumes


def test_a_missing_dump_is_refused(tmp_path: Path) -> None:
    gone = tm.Moment("dump", "x", "2026-01-01T00:00:00+00:00", str(tmp_path / "no.dump"), 5)
    with pytest.raises(tm.TimeMachineError, match="gone"):
        tm.start(gone, sh=FakeDocker(), record_path=tmp_path / "rec.json")


def test_unsupported_moment_kind_is_refused(tmp_path: Path) -> None:
    m = tm.Moment("pitr", "x", "2026-01-01T00:00:00+00:00", "/x", 5)
    with pytest.raises(tm.TimeMachineError, match="not supported"):
        tm.start(m, sh=FakeDocker(), record_path=tmp_path / "rec.json")


def test_the_port_is_never_a_live_port() -> None:
    for _ in range(20):
        assert tm.pick_port() not in tm.LIVE_PORTS
    # even if the kernel kept handing back a live port the picker would refuse, not use it
    with pytest.raises(tm.TimeMachineError):
        tm.pick_port(taken=frozenset(range(1, 65536)))


def test_extend_once_only(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()
    rec = go(dump, d, tmp_path)
    before = rec.expires_at
    again = tm.extend(record_path=tmp_path / "rec.json", now=lambda: before - 100)
    assert again.expires_at == before + tm.TTL_SECS and again.extensions == 1
    with pytest.raises(tm.TimeMachineError, match="already extended"):
        tm.extend(record_path=tmp_path / "rec.json")
    with pytest.raises(tm.TimeMachineError, match="no throwaway"):
        tm.extend(record_path=tmp_path / "none.json")


def test_reap_leaves_a_live_throwaway_and_removes_an_expired_one(
        dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()
    rec = go(dump, d, tmp_path)
    p = tmp_path / "rec.json"
    assert tm.reap(sh=d, record_path=p, now=lambda: rec.expires_at - 1) == []
    assert rec.container in d.containers
    removed = tm.reap(sh=d, record_path=p, now=lambda: rec.expires_at + 1)
    assert f"container {rec.container}" in removed
    assert not d.containers and not d.volumes and tm.read_record(p) is None


def test_reap_sweeps_orphans_even_while_one_is_live(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()
    rec = go(dump, d, tmp_path)
    d.containers.add("osiris-tm-deadbeef")
    d.volumes.add("osiris-tm-vol-deadbeef")
    removed = tm.reap(sh=d, record_path=tmp_path / "rec.json", now=lambda: rec.expires_at - 5)
    assert "container osiris-tm-deadbeef" in removed
    assert "volume osiris-tm-vol-deadbeef" in removed
    assert rec.container in d.containers and rec.volume in d.volumes


def test_teardown_never_touches_the_live_container_or_volume(tmp_path: Path) -> None:
    d = FakeDocker(live_volumes="osiris-tm-vol-livelook")
    d.containers |= {"osiris-pg", "osiris-tm-aa"}
    d.volumes |= {"osiris-tm-vol-livelook", "osiris-tm-vol-aa", "somethingelse"}
    tm.teardown(sh=d, record_path=tmp_path / "rec.json")
    assert "osiris-pg" in d.containers
    assert "osiris-tm-vol-livelook" in d.volumes and "somethingelse" in d.volumes
    assert "osiris-tm-aa" not in d.containers and "osiris-tm-vol-aa" not in d.volumes


def test_failed_record_is_reaped_before_its_ttl(dump: tm.Moment, tmp_path: Path) -> None:
    d = FakeDocker()
    rec = go(dump, d, tmp_path)
    rec.state = "failed"
    tm._write_record(rec, tmp_path / "rec.json")
    tm.reap(sh=d, record_path=tmp_path / "rec.json", now=lambda: rec.started_at + 1)
    assert not d.containers and tm.read_record(tmp_path / "rec.json") is None


def test_free_bytes_walks_up_from_an_unreadable_directory(tmp_path: Path) -> None:
    assert tm.free_bytes(tmp_path / "no" / "such" / "dir") > 0
