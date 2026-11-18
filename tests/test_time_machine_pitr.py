"""Point-in-time moments: windows from the local vault, refusals, and the recovery steps."""
from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import time_machine as tm
from src.orchestrator import time_machine_pitr as pitr

BASE = "osiris-basebackup-20261115-201856.tar.gz"
ID = "pitr-osiris-basebackup-20261115-201856"


def _t(day: int, hour: int = 0) -> float:
    return datetime(2026, 11, day, hour, tzinfo=UTC).timestamp()


def _seg(n: int) -> str:
    return f"00000001{0:08X}{n:08X}"


@pytest.fixture
def vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A vault with a base backup (Nov 15 20:00) and an unbroken run of WAL after it (to
    Nov 17 20:00), then a gap, then a second base (Nov 19 00:00) with its own short run."""
    v = tmp_path / "vault"
    (v / "basebackups").mkdir(parents=True)
    (v / "wal_archive").mkdir()
    base = v / "basebackups" / BASE
    base.write_bytes(b"b" * 3000)
    os.utime(base, (_t(15, 20), _t(15, 20)))
    for i in range(1, 25):                       # one segment every two hours, Nov 15 22:00 ->
        p = v / "wal_archive" / _seg(i)
        p.write_bytes(b"w")
        os.utime(p, (_t(15, 20) + i * 7200,) * 2)
    for i in range(40, 43):                      # a gap in numbering: a second run
        p = v / "wal_archive" / _seg(i)
        p.write_bytes(b"w")
        os.utime(p, (_t(19, 0) + (i - 40) * 7200,) * 2)
    base2 = v / "basebackups" / "osiris-basebackup-20261119-000000.tar.gz"
    base2.write_bytes(b"c" * 2000)
    os.utime(base2, (_t(19, 0), _t(19, 0)))
    monkeypatch.setattr(tm, "VAULT_DIR", v)
    monkeypatch.setenv("OSIRIS_RESTORE_POINTS_FILE", str(tmp_path / "none.json"))
    return v


def test_windows_come_from_the_files_on_this_machine(vault: Path) -> None:
    got = pitr.local_windows(vault)
    bases = [w["base"] for w in got["windows"]]
    assert bases == [BASE, "osiris-basebackup-20261119-000000.tar.gz"]
    assert len(got["gaps"]) == 1
    m = next(i for i in pitr.pitr_moments(vault) if i["id"] == ID)
    assert m["available"] and m["pick_time"] and m["size_bytes"] == 3000


def test_a_nas_only_window_is_listed_with_whether_it_can_be_opened_now(vault: Path) -> None:
    cache = {"windows": [{"from": "2026-10-19T02:01:38+00:00", "to": "2026-11-01T23:15:07+00:00",
                          "base": "osiris-basebackup-20261018-205720.tar.gz"}]}
    away = pitr.pitr_moments(vault, cache, presence=lambda _t: {"present": False,
                                                              "reason": "reachable only over vpn"})
    old = next(i for i in away if i["id"].endswith("20261018-205720"))
    assert old["available"] is False and "built yet" in old["reason"]    # no NAS map saved yet
    assert next(i for i in away if i["id"] == ID)["available"] is True


def test_a_time_inside_the_window_gives_a_moment(vault: Path) -> None:
    m = pitr.find_pitr_moment(ID, "2026-11-16T12:00:00Z", vault)
    assert m.kind == "pitr" and m.at == "2026-11-16T12:00:00+00:00"
    assert m.path.endswith(BASE) and m.window_from and m.window_to


@pytest.mark.parametrize("at,words", [
    ("2026-11-18T03:00:00Z", "falls in a gap"),
    ("2026-11-01T00:00:00Z", "before the oldest"),
    ("2027-01-01T00:00:00Z", "after the newest"),
])
def test_a_time_outside_a_window_is_refused_with_the_nearest_times(
        vault: Path, at: str, words: str) -> None:
    with pytest.raises(tm.TimeMachineError) as err:
        pitr.find_pitr_moment(ID, at, vault)
    text = str(err.value)
    assert words in text and "UTC" in text
    if "gap" in words:
        assert "nearest restorable times are" in text


def test_a_time_inside_a_nas_only_window_gives_a_nas_moment(
        vault: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    cache = tmp_path / "rp.json"
    cache.write_text(json.dumps({"windows": [
        {"from": "2026-10-19T02:01:38+00:00", "to": "2026-11-01T23:15:07+00:00",
         "base": "osiris-basebackup-20261018-205720.tar.gz"}], "gaps": []}))
    monkeypatch.setenv("OSIRIS_RESTORE_POINTS_FILE", str(cache))
    m = pitr.find_pitr_moment("pitr-osiris-basebackup-20261018-205720",
                              "2026-10-25T00:00:00Z", vault)
    assert m.kind == "pitr" and m.path == "" and m.repo_path.endswith("205720.tar.gz")  # type: ignore[union-attr]
    assert m.at == "2026-10-25T00:00:00+00:00" and "NAS" in m.label
    with pytest.raises(tm.TimeMachineError, match="outside that window"):
        pitr.find_pitr_moment("pitr-osiris-basebackup-20261018-205720",
                              "2026-11-16T12:00:00Z", vault)


def test_missing_or_unreadable_time_and_unknown_window_are_sentences(vault: Path) -> None:
    with pytest.raises(tm.TimeMachineError, match="Pick the time"):
        pitr.find_pitr_moment(ID, None, vault)
    with pytest.raises(tm.TimeMachineError, match="not a time"):
        pitr.find_pitr_moment(ID, "yesterday-ish", vault)
    with pytest.raises(tm.TimeMachineError, match="no such moment"):
        pitr.find_pitr_moment("pitr-osiris-basebackup-19990101-000000",
                              "2026-11-16T12:00:00Z", vault)


def test_find_moment_routes_a_pitr_id_with_its_time(vault: Path) -> None:
    m = tm.find_moment(ID, vault, at="2026-11-16T12:00:00Z")
    assert m.kind == "pitr" and m.at
    with pytest.raises(tm.TimeMachineError, match="Pick the time"):
        tm.find_moment(ID, vault)


def test_the_recovery_settings_target_the_time_and_never_archive(vault: Path) -> None:
    conf = pitr.recovery_conf(vault / "wal_archive", "2026-11-16T12:00:00+00:00")
    assert "recovery_target_time = '2026-11-16T12:00:00+00:00'" in conf
    assert "recovery_target_action = 'promote'" in conf and "restore_command" in conf
    args = pitr.recovery_server_args()
    assert "archive_mode=off" in args
    assert f"max_connections={pitr.RECOVERY_MAX_CONNECTIONS}" in args


class FakeDocker:
    def __init__(self, *, recovery_polls: int = 2, die_in_recovery: bool = False) -> None:
        self.calls: list[list[str]] = []
        self.containers: set[str] = set()
        self.volumes: set[str] = set()
        self.polls = recovery_polls
        self.die = die_in_recovery

    def __call__(self, cmd: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
        self.calls.append(cmd)
        j = " ".join(cmd)
        out = ""
        if cmd[:3] == ["docker", "volume", "create"]:
            self.volumes.add(cmd[-1])
        elif cmd[:3] == ["docker", "volume", "rm"]:
            self.volumes.discard(cmd[-1])
        elif cmd[:3] == ["docker", "run", "-d"]:
            self.containers.add(cmd[cmd.index("--name") + 1])
        elif cmd[:3] == ["docker", "rm", "-f"]:
            self.containers.discard(cmd[-1])
        elif cmd[:3] == ["docker", "ps", "-a"]:
            out = "\n".join(self.containers)
        elif cmd[:3] == ["docker", "volume", "ls"]:
            out = "\n".join(self.volumes)
        elif cmd[:2] == ["docker", "inspect"] and "State.Status" in j:
            out = "exited" if self.die else "running"
        elif cmd[:2] == ["docker", "inspect"]:
            out = "osiris-pg:16-repack" if "Config.Image" in j else "livevol"
        elif cmd[:2] == ["docker", "info"]:
            out = "/var/lib/docker"
        elif cmd[:2] == ["docker", "logs"]:
            return subprocess.CompletedProcess(cmd, 0, "", "FATAL: recovery ended before target")
        elif "pg_is_in_recovery" in j:
            self.polls -= 1
            out = "t" if self.polls > 0 and not self.die else ("f" if not self.die else "t")
        elif "SELECT count(*)" in j:
            out = "7"
        return subprocess.CompletedProcess(cmd, 0, out + "\n", "")


def _moment(vault: Path) -> tm.Moment:
    return pitr.find_pitr_moment(ID, "2026-11-16T12:00:00Z", vault)


def test_start_unpacks_prepares_recovery_waits_for_the_open_and_adds_the_reader(
        vault: Path, tmp_path: Path) -> None:
    d = FakeDocker()
    rec = tm.start(_moment(vault), sh=d, record_path=tmp_path / "rec.json",
                   sleep=lambda _s: None, free=lambda _p: 10**12)
    assert rec.state == "ready" and rec.container in d.containers
    kinds = [c for c in d.calls if c[:2] == ["docker", "run"]]
    unpack, prepare, server = kinds
    assert "tar -xzf /tm/base.tar.gz -C /data" in " ".join(unpack) and "--rm" in unpack
    assert any(a.startswith("TM_CONF=") and "recovery_target_time" in a for a in prepare)
    pub = server[server.index("-p") + 1]
    assert pub.startswith("127.0.0.1:") and int(pub.split(":")[1]) not in tm.LIVE_PORTS
    assert "archive_mode=off" in server
    assert any(a.endswith(":ro") and str(vault / "wal_archive") in a for a in server)
    assert not any("livevol" in a for a in server)
    # the reader role was added only AFTER the server left recovery
    j = [" ".join(c) for c in d.calls]
    assert max(i for i, c in enumerate(j) if "pg_is_in_recovery" in c) < \
        next(i for i, c in enumerate(j) if "CREATE ROLE" in c)


def test_a_recovery_that_cannot_reach_the_time_is_a_sentence_and_cleans_up(
        vault: Path, tmp_path: Path) -> None:
    d = FakeDocker(die_in_recovery=True)
    with pytest.raises(tm.TimeMachineError, match="stopped while replaying"):
        tm.start(_moment(vault), sh=d, record_path=tmp_path / "rec.json",
                 sleep=lambda _s: None, free=lambda _p: 10**12)
    assert not d.containers and not d.volumes and tm.read_record(tmp_path / "rec.json") is None


def test_a_recovery_that_never_finishes_is_bounded(vault: Path, tmp_path: Path) -> None:
    d = FakeDocker(recovery_polls=10**9)
    clock = iter(range(0, 10**7, 600))
    with pytest.raises(tm.TimeMachineError, match="had not finished"):
        tm.start(_moment(vault), sh=d, record_path=tmp_path / "rec.json",
                 sleep=lambda _s: None, free=lambda _p: 10**12, now=lambda: float(next(clock)))
    assert not d.containers and not d.volumes


def test_the_disk_check_scales_for_an_unpacked_base_backup(
        vault: Path, tmp_path: Path) -> None:
    d = FakeDocker()
    with pytest.raises(tm.TimeMachineError, match="not enough disk"):
        tm.start(_moment(vault), sh=d, record_path=tmp_path / "rec.json",
                 free=lambda _p: 3000 * 5)        # enough for 3x, not for 8x
    assert not d.containers and not d.volumes
    assert tm.disk_problem(3000, 3000 * 8, factor=tm.PITR_DISK_FACTOR) is None


@pytest.mark.asyncio
async def test_the_listing_merges_dumps_and_windows(vault: Path) -> None:
    from src.orchestrator import time_machine_console as tmc

    (vault / "osiris-20261115-201856.dump").write_bytes(b"d")
    got: Any = (await tmc.moments())["moments"]
    kinds = {m["kind"] for m in got}
    assert kinds == {"dump", "pitr"} and all("available" in m for m in got)


def test_a_container_that_vanishes_during_recovery_fails_fast(
        vault: Path, tmp_path: Path) -> None:
    class Vanishing(FakeDocker):
        def __call__(self, cmd: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
            if cmd[:2] == ["docker", "inspect"] and "State.Status" in " ".join(cmd):
                self.calls.append(cmd)
                return subprocess.CompletedProcess(cmd, 1, "", "No such object")
            return super().__call__(cmd, timeout)

    d = Vanishing(recovery_polls=10**9)
    with pytest.raises(tm.TimeMachineError, match="is gone"):
        tm.start(_moment(vault), sh=d, record_path=tmp_path / "rec.json",
                 sleep=lambda _s: None, free=lambda _p: 10**12)
    assert not d.containers and not d.volumes


def test_the_server_log_gist_keeps_the_lines_that_say_what_went_wrong() -> None:
    fatal = "FATAL: recovery ended before configured recovery target was reached"
    log = f"LOG: ok\nLOG: redo done\n{fatal}\nLOG: terminating"
    assert pitr._log_gist(log) == fatal
    assert pitr._log_gist("a\nb\nc\nd") == "b | c | d"


def test_the_recovery_state_query_spells_the_flag_out_and_the_wait_reads_both_spellings(
        vault: Path, tmp_path: Path) -> None:
    assert "CASE WHEN pg_is_in_recovery() THEN 't' ELSE 'f' END" in pitr.RECOVERY_STATE_SQL

    class Spelled(FakeDocker):
        def __call__(self, cmd: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
            if "pg_is_in_recovery" in " ".join(cmd) and "COALESCE" in " ".join(cmd):
                self.calls.append(cmd)
                self.polls -= 1
                word = "true" if self.polls > 0 else "false"       # what a boolean joined to
                return subprocess.CompletedProcess(                  # text really prints
                    cmd, 0, f"{word}|2026-10-22 11:59:57+00\n", "")
            return super().__call__(cmd, timeout)

    seen: list[str] = []
    d = Spelled(recovery_polls=3)
    rec = tm.start(_moment(vault), sh=d, record_path=tmp_path / "rec.json",
                   sleep=lambda _s: None, free=lambda _p: 10**12,
                   on_phase=lambda r: seen.append(r.progress))
    assert rec.state == "ready"
    assert any(t.startswith("replaying: reached Oct 22 11:59") for t in seen)
